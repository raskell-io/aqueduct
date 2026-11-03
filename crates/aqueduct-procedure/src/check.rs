//! Static checks on a procedure's syntax tree, before it can be registered (BRIEF.md section 4.4, ADR
//! 0037).
//!
//! **Recursion is rejected**: it is what makes termination a matter of the statement budget alone. A
//! function that can reach itself through calls is an error. The check is conservative where functions
//! are values: a function used other than by calling it by name (passed to `sorted`, stored in a dict, a
//! lambda) *escapes*, and a call of anything that is not a known function's name (a parameter, `f[0]()`)
//! may call any escaped function. Method calls (`text.lower()`) call built-in methods only: standard
//! Starlark has no user-defined attributes.
//!
//! **Lints** are warnings: the procedure compiles, and they are reported where it is checked and
//! registered. Each can be silenced on its line with `# starlark-lint-disable <name>`:
//!
//! - `unbounded-wait`: `wait(...)` without a timeout; the run may wait forever;
//! - `unused-escalation`: the resolution of `escalate(...)` is not used;
//! - `unchecked-failure`: the result of a `fallible = True` call is not used, so its failure is ignored;
//! - `unanswered-update`: a received update is dropped, or the procedure never answers updates.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use starlark::codemap::Span;
use starlark::environment::Globals;
use starlark::syntax::AstModule;
use starlark::syntax::ast::{
    Argument, AssignTarget, AstExpr, AstStmt, Expr, ForClause, Parameter, Stmt,
};
use starlark_syntax::syntax::uniplate::Visit;

/// A warning about a procedure that compiles.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lint {
    /// The lint's name, as used to silence it (for example `unbounded-wait`).
    pub name: &'static str,
    /// The line it is about, from 1.
    pub line: usize,
    /// What is wrong, and why it matters.
    pub message: String,
}

impl fmt::Display for Lint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "line {}: {} ({})", self.line, self.message, self.name)
    }
}

/// What one function does: whom it calls and references by name, and whether it calls values.
#[derive(Default)]
struct Function {
    /// Names called, with the line of the call.
    calls: Vec<(String, usize)>,
    /// Names used as values.
    refs: Vec<(String, usize)>,
    /// Lines of calls of something that is not a name.
    dynamic: Vec<usize>,
    /// Defined at this line (0 for the module's top level).
    line: usize,
}

struct Walker<'a> {
    ast: &'a AstModule,
    functions: BTreeMap<String, Function>,
    /// Names bound by assignments, parameters and loops: such a name may hold a function value.
    bound: BTreeSet<String>,
    lints: Vec<Lint>,
    anonymous: usize,
    /// Lines of `update(...)` calls, and whether anything answers updates.
    updates: Vec<usize>,
    answers: bool,
}

const MODULE: &str = "<module>";

/// Checks a parsed procedure: an error if it recurses, otherwise its lints.
pub(crate) fn check(ast: &AstModule, globals: &Globals) -> Result<Vec<Lint>, String> {
    let mut walker = Walker {
        ast,
        functions: BTreeMap::new(),
        bound: BTreeSet::new(),
        lints: Vec::new(),
        anonymous: 0,
        updates: Vec::new(),
        answers: false,
    };
    walker
        .functions
        .insert(MODULE.to_owned(), Function::default());
    walker.stmt(ast.statement(), MODULE);
    let builtins: BTreeSet<String> = globals.names().map(|n| n.as_str().to_owned()).collect();
    if let Some(cycle) = walker.cycle(&builtins) {
        return Err(format!(
            "recursion is not allowed, so that every activation ends: {cycle}"
        ));
    }
    if !walker.answers {
        for line in walker.updates.clone() {
            walker.lint(
                "unanswered-update",
                line,
                "the procedure waits for updates but never answers one with respond(...) or reject(...); \
                 senders wait until the run ends"
                    .into(),
            );
        }
    }
    let ast = walker.ast;
    let mut lints = walker.lints;
    lints.retain(|lint| {
        let span = ast.statement().span;
        // Suppressions are per line: `# starlark-lint-disable <name>` on the lint's line or the one
        // before it.
        let line_span = line_span(ast, lint.line).unwrap_or(span);
        !ast.is_suppressed(lint.name, line_span)
    });
    lints.sort_by_key(|lint| lint.line);
    Ok(lints)
}

/// The span of a line (from 1), if the module has it.
fn line_span(ast: &AstModule, line: usize) -> Option<Span> {
    let codemap = ast.file_span(ast.statement().span).file;
    codemap.line_span_opt(line.checked_sub(1)?)
}

impl Walker<'_> {
    fn line(&self, span: Span) -> usize {
        self.ast.file_span(span).resolve_span().begin.line + 1
    }

    fn lint(&mut self, name: &'static str, line: usize, message: String) {
        self.lints.push(Lint {
            name,
            line,
            message,
        });
    }

    fn function(&mut self, name: &str) -> &mut Function {
        self.functions.entry(name.to_owned()).or_default()
    }

    fn stmt(&mut self, stmt: &AstStmt, current: &str) {
        match &stmt.node {
            Stmt::Def(def) => {
                let name = def.name.node.ident.clone();
                self.bound.remove(&name);
                for param in &def.params {
                    self.param(&param.node, current);
                }
                let line = self.line(stmt.span);
                self.function(&name).line = line;
                self.stmt(&def.body, &name);
            }
            Stmt::Assign(assign) => {
                self.target(&assign.lhs.node, current);
                match (&assign.lhs.node, &assign.rhs.node) {
                    // A named lambda is a function of that name.
                    (AssignTarget::Identifier(ident), Expr::Lambda(lambda)) => {
                        let name = ident.node.ident.clone();
                        self.bound.remove(&name);
                        let line = self.line(assign.rhs.span);
                        self.function(&name).line = line;
                        for param in &lambda.params {
                            self.param(&param.node, current);
                        }
                        self.expr(&lambda.body, &name);
                    }
                    _ => self.expr(&assign.rhs, current),
                }
            }
            Stmt::AssignModify(target, _, rhs) => {
                self.target(&target.node, current);
                self.expr(rhs, current);
            }
            Stmt::For(each) => {
                self.target(&each.var.node, current);
                self.expr(&each.over, current);
                self.stmt(&each.body, current);
            }
            Stmt::Expression(expr) => {
                self.unused(expr);
                self.expr(expr, current);
            }
            _ => stmt.node.visit_children(|child| match child {
                Visit::Stmt(child) => self.stmt(child, current),
                Visit::Expr(child) => self.expr(child, current),
            }),
        }
    }

    /// Names bound by an assignment target; expressions in it (indexes) are walked.
    fn target(&mut self, target: &AssignTarget, current: &str) {
        match target {
            AssignTarget::Identifier(ident) => {
                self.bound.insert(ident.node.ident.clone());
            }
            AssignTarget::Tuple(targets) => {
                for target in targets {
                    self.target(&target.node, current);
                }
            }
            AssignTarget::Index(index) => {
                self.expr(&index.0, current);
                self.expr(&index.1, current);
            }
            AssignTarget::Dot(object, _) => self.expr(object, current),
        }
    }

    fn param(&mut self, param: &Parameter, current: &str) {
        match param {
            Parameter::Normal(name, _, default) => {
                self.bound.insert(name.node.ident.clone());
                if let Some(default) = default {
                    self.expr(default, current);
                }
            }
            Parameter::Args(name, _) | Parameter::KwArgs(name, _) => {
                self.bound.insert(name.node.ident.clone());
            }
            _ => {}
        }
    }

    fn expr(&mut self, expr: &AstExpr, current: &str) {
        let line = self.line(expr.span);
        match &expr.node {
            Expr::Identifier(ident) => {
                let name = ident.node.ident.clone();
                self.function(current).refs.push((name, line));
            }
            Expr::Lambda(lambda) => {
                // An anonymous lambda is a value: it escapes.
                self.anonymous += 1;
                let name = format!("<lambda {}>", self.anonymous);
                self.function(&name).line = line;
                self.function(current).refs.push((name.clone(), line));
                for param in &lambda.params {
                    self.param(&param.node, current);
                }
                self.expr(&lambda.body, &name);
            }
            Expr::Call(callee, args) => {
                match &callee.node {
                    Expr::Identifier(ident) => {
                        let name = ident.node.ident.as_str();
                        match name {
                            "update" => self.updates.push(line),
                            "respond" | "reject" => self.answers = true,
                            "wait" => self.wait(&args.args, line),
                            _ => {}
                        }
                        self.function(current).calls.push((name.to_owned(), line));
                    }
                    // A built-in method (`text.lower()`); the object is walked.
                    Expr::Dot(object, _) => self.expr(object, current),
                    _ => {
                        self.function(current).dynamic.push(line);
                        self.expr(callee, current);
                    }
                }
                for arg in &args.args {
                    match &arg.node {
                        Argument::Positional(value)
                        | Argument::Named(_, value)
                        | Argument::Args(value)
                        | Argument::KwArgs(value) => self.expr(value, current),
                    }
                }
            }
            Expr::ListComprehension(_, first, _) | Expr::DictComprehension(_, first, _) => {
                self.comprehension(first);
                expr.node.visit_expr(|child| self.expr(child, current));
            }
            _ => expr.node.visit_expr(|child| self.expr(child, current)),
        }
    }

    fn comprehension(&mut self, clause: &ForClause) {
        self.target_names(&clause.var.node);
    }

    fn target_names(&mut self, target: &AssignTarget) {
        match target {
            AssignTarget::Identifier(ident) => {
                self.bound.insert(ident.node.ident.clone());
            }
            AssignTarget::Tuple(targets) => {
                for target in targets {
                    self.target_names(&target.node);
                }
            }
            _ => {}
        }
    }

    fn wait(&mut self, args: &[starlark::syntax::ast::AstArgument], line: usize) {
        let timeout = args
            .iter()
            .any(|arg| matches!(&arg.node, Argument::Named(name, _) if name.node == "timeout"));
        if !timeout {
            self.lint(
                "unbounded-wait",
                line,
                "wait(...) has no timeout: the run may wait forever; give it `timeout = ...`"
                    .into(),
            );
        }
    }

    /// Lints for an expression statement: a result that should have been used.
    fn unused(&mut self, expr: &AstExpr) {
        let Expr::Call(callee, args) = &expr.node else {
            return;
        };
        let Expr::Identifier(ident) = &callee.node else {
            return;
        };
        let line = self.line(expr.span);
        let fallible = args.args.iter().any(|arg| {
            matches!(&arg.node, Argument::Named(name, value)
                if name.node == "fallible"
                    && matches!(&value.node, Expr::Identifier(v) if v.node.ident == "True"))
        });
        let waits_for_update = args.args.first().is_some_and(|arg| {
            matches!(&arg.node, Argument::Positional(value)
                if matches!(&value.node, Expr::Call(inner, _)
                    if matches!(&inner.node, Expr::Identifier(i) if i.node.ident == "update")))
        });
        match ident.node.ident.as_str() {
            "escalate" => self.lint(
                "unused-escalation",
                line,
                "the resolution of escalate(...) is not used: the run waits for a decision and then ignores it"
                    .into(),
            ),
            "step" | "call" if fallible => self.lint(
                "unchecked-failure",
                line,
                format!(
                    "the result of {}(..., fallible = True) is not used, so its failure is ignored",
                    ident.node.ident
                ),
            ),
            "wait" if waits_for_update => self.lint(
                "unanswered-update",
                line,
                "the update wait(update(...)) receives is dropped: it can never be answered".into(),
            ),
            _ => {}
        }
    }

    /// A cycle in the call graph, described, if there is one.
    fn cycle(&self, builtins: &BTreeSet<String>) -> Option<String> {
        let known: BTreeSet<&str> = self.functions.keys().map(String::as_str).collect();
        // Functions used as values, and anonymous lambdas.
        let escaped: BTreeSet<&str> = self
            .functions
            .values()
            .flat_map(|f| f.refs.iter())
            .map(|(name, _)| name.as_str())
            .filter(|name| known.contains(name) && *name != MODULE)
            .collect();
        let mut edges: BTreeMap<&str, Vec<(&str, usize)>> = BTreeMap::new();
        for (name, function) in &self.functions {
            let out = edges.entry(name.as_str()).or_default();
            for (callee, line) in function.calls.iter().chain(&function.refs) {
                if known.contains(callee.as_str()) {
                    out.push((callee.as_str(), *line));
                }
            }
            // A call of a name that may hold a function value, or of an expression, may call any
            // escaped function.
            let dynamic = function.dynamic.iter().copied().chain(
                function
                    .calls
                    .iter()
                    .filter(|(callee, _)| {
                        !known.contains(callee.as_str())
                            && (self.bound.contains(callee) || !builtins.contains(callee))
                    })
                    .map(|(_, line)| *line),
            );
            for line in dynamic {
                out.extend(escaped.iter().map(|target| (*target, line)));
            }
        }
        // Depth-first search, reporting the first cycle found.
        let mut done: BTreeSet<&str> = BTreeSet::new();
        for start in edges.keys() {
            let mut path: Vec<(&str, usize)> = Vec::new();
            if let Some(cycle) = visit(start, 0, &edges, &mut path, &mut done) {
                return Some(cycle);
            }
        }
        None
    }
}

fn visit<'a>(
    node: &'a str,
    line: usize,
    edges: &BTreeMap<&'a str, Vec<(&'a str, usize)>>,
    path: &mut Vec<(&'a str, usize)>,
    done: &mut BTreeSet<&'a str>,
) -> Option<String> {
    if let Some(position) = path.iter().position(|(n, _)| *n == node) {
        let mut cycle = path[position..].to_vec();
        cycle.push((node, line));
        let mut text = format!("`{}`", cycle[0].0);
        for (name, line) in &cycle[1..] {
            text.push_str(&format!(" calls `{name}` (line {line})"));
            if *name != cycle[0].0 {
                text.push(',');
            }
        }
        return Some(text);
    }
    if done.contains(node) {
        return None;
    }
    path.push((node, line));
    for (next, line) in edges.get(node).into_iter().flatten() {
        if let Some(cycle) = visit(next, *line, edges, path, done) {
            return Some(cycle);
        }
    }
    path.pop();
    done.insert(node);
    None
}
