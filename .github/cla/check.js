// CLA check for Aqueduct pull requests. Run by .github/workflows/cla.yml through actions/github-script.
//
// Signers are every PR author and every commit author of the pull request. A signer signs by
// commenting the exact phrase below; the signature is stored (by GitHub account id, which survives
// renames) in signatures.json on the orphan branch `cla-signatures`. The result is reported as the
// commit status `CLA` on the pull request's head commit, plus one comment that is kept up to date.

const PHRASE = 'I have read the Aqueduct CLA and I hereby sign it.';
const CLA_VERSION = '1.0';
const BRANCH = 'cla-signatures';
const FILE = 'signatures.json';
const MARKER = '<!-- aqueduct-cla -->';
const EXEMPT = new Set(['raffaelschneider']); // the Maintainer

module.exports = async ({ github, context }) => {
  const { owner, repo } = context.repo;
  const number = context.payload.pull_request?.number ?? context.payload.issue?.number;
  const { data: pr } = await github.rest.pulls.get({ owner, repo, pull_number: number });

  // Who has to sign.
  const required = new Map(); // login -> account id
  const unlinked = new Set(); // commit authors whose email is not linked to a GitHub account
  const consider = (user) => {
    if (user.type === 'Bot' || EXEMPT.has(user.login)) return;
    required.set(user.login, user.id);
  };
  consider(pr.user);
  const commits = await github.paginate(github.rest.pulls.listCommits, {
    owner, repo, pull_number: number, per_page: 100,
  });
  for (const commit of commits) {
    if (commit.author) consider(commit.author);
    else unlinked.add(commit.commit.author.name);
  }

  // Who has signed.
  let { sha, signatures } = await load(github, owner, repo);
  const signedIds = new Set(signatures.map((s) => s.id));

  // A signing comment from someone who has to sign.
  if (context.eventName === 'issue_comment') {
    const comment = context.payload.comment;
    const signer = comment.user;
    if (comment.body.trim() === PHRASE && required.has(signer.login) && !signedIds.has(signer.id)) {
      signatures.push({
        login: signer.login,
        id: signer.id,
        cla_version: CLA_VERSION,
        signed_at: comment.created_at,
        pull_request: number,
        comment_url: comment.html_url,
      });
      await github.rest.repos.createOrUpdateFileContents({
        owner, repo, branch: BRANCH, path: FILE, sha,
        message: `CLA ${CLA_VERSION} signed by ${signer.login} (#${number})`,
        content: Buffer.from(JSON.stringify(signatures, null, 2) + '\n').toString('base64'),
      });
      signedIds.add(signer.id);
    }
  }

  const missing = [...required].filter(([, id]) => !signedIds.has(id)).map(([login]) => login);
  const ok = missing.length === 0 && unlinked.size === 0;

  await github.rest.repos.createCommitStatus({
    owner, repo, sha: pr.head.sha,
    state: ok ? 'success' : 'failure',
    context: 'CLA',
    description: ok ? 'All contributors have signed the CLA' : 'Contributors still need to sign the CLA',
    target_url: `https://github.com/${owner}/${repo}/blob/main/CLA.md`,
  });

  // Keep one comment up to date; only create it when something is missing.
  const comments = await github.paginate(github.rest.issues.listComments, {
    owner, repo, issue_number: number, per_page: 100,
  });
  const existing = comments.find((c) => c.user.type === 'Bot' && c.body.includes(MARKER));
  let body;
  if (ok) {
    body = `${MARKER}\nAll contributors to this pull request have signed the [Aqueduct CLA](https://github.com/${owner}/${repo}/blob/main/CLA.md). Thank you.`;
  } else {
    const lines = [
      MARKER,
      `Thank you for your contribution. Before it can be merged, every contributor needs to sign the [Aqueduct Contributor License Agreement](https://github.com/${owner}/${repo}/blob/main/CLA.md) (version ${CLA_VERSION}).`,
      '',
      'To sign, read the agreement and comment on this pull request with exactly:',
      '',
      '```text',
      PHRASE,
      '```',
      '',
    ];
    if (missing.length) lines.push(`Still to sign: ${missing.map((l) => `@${l}`).join(', ')}.`);
    if (unlinked.size) {
      lines.push(
        `Some commits are authored with an email address that is not linked to a GitHub account (${[...unlinked].join(', ')}). ` +
          'Add the email address to your GitHub account, or amend the commits, so the signature can be matched.',
      );
    }
    lines.push('', `If you contribute on behalf of an employer, your employer also needs to sign the [Corporate CLA](https://github.com/${owner}/${repo}/blob/main/CLA-CORPORATE.md).`);
    body = lines.join('\n');
  }
  if (existing) {
    if (existing.body !== body) {
      await github.rest.issues.updateComment({ owner, repo, comment_id: existing.id, body });
    }
  } else if (!ok) {
    await github.rest.issues.createComment({ owner, repo, issue_number: number, body });
  }
};

// Loads signatures.json from the signatures branch, creating the branch (as an orphan) if needed.
async function load(github, owner, repo) {
  try {
    const { data } = await github.rest.repos.getContent({ owner, repo, path: FILE, ref: BRANCH });
    return { sha: data.sha, signatures: JSON.parse(Buffer.from(data.content, 'base64').toString('utf8')) };
  } catch (error) {
    if (error.status !== 404) throw error;
  }
  const { data: blob } = await github.rest.git.createBlob({ owner, repo, content: '[]\n', encoding: 'utf-8' });
  const { data: tree } = await github.rest.git.createTree({
    owner, repo, tree: [{ path: FILE, mode: '100644', type: 'blob', sha: blob.sha }],
  });
  const { data: commit } = await github.rest.git.createCommit({
    owner, repo, message: 'Start the CLA signature record', tree: tree.sha, parents: [],
  });
  await github.rest.git.createRef({ owner, repo, ref: `refs/heads/${BRANCH}`, sha: commit.sha });
  return { sha: blob.sha, signatures: [] };
}
