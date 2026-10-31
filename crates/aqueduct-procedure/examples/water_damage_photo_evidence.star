# The claims procedure from BRIEF.md section 4.4, as a Starlark procedure.
# Not yet supported in the phase 0 spike: wait() for signals with timeouts, call() for child runs and
# a real escalation outcome. Here the customer's photos arrive through a step, settlement is a step,
# and escalate() ends the run.

PROCEDURE = {
    "name": "claims/water-damage-photo-evidence",
    "version": 3,
    "origin": {"explored_by": "agent/claims-explorer", "cases_studied": 37},
}

def run(claim):
    docs = agent(
        "classify_documents",
        claim["documents"],
        prompt = "prompts/classify-documents.md",
        output_schema = {"$ref": "schemas/DocumentClassification.json"},
        budget_tokens = 20000,
    )

    coverage = step("coverage.water_damage@v7", claim)
    if not coverage["covered"]:
        fail("not covered: " + coverage["reason"])

    if docs["missing_photos"]:
        step("notify.customer", {"template": "claims/missing-photos", "to": claim["customer"]})
        photos = step("customer.photos", claim["id"], fallible = True)
        if not photos["ok"]:
            escalate("no-photos", claim)

    if claim["amount"] > 5000 or docs["confidence"] < 0.8:
        escalate("manual-review", claim)

    return step("claims.settle@v2", {"claim": claim["id"], "coverage": coverage})
