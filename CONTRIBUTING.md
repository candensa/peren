# Contributing to Peren

Peren welcomes bug reports, documentation improvements, compatibility evidence, focused feature proposals and code contributions.

Peren is a runtime for stateful and multi-tenant workloads. Changes can affect persisted data, tenant isolation, provider boundaries, fleet membership and recovery. Contributions should therefore define the behavior they change and provide evidence at the boundary users depend on.

## Before you begin

- Read [DEVELOPMENT.md](DEVELOPMENT.md) before changing code, tests, packaging or public documentation.
- Search [existing issues](https://github.com/candensa/peren/issues) before opening a new report or proposal.
- Do not include credentials, tenant data, private endpoints, production configuration or proprietary Worker code in an issue or pull request.

## Ways to contribute

You can help by:

- reporting a reproducible defect;
- improving an incomplete or unclear documentation path;
- adding a focused example;
- correcting a compatibility statement;
- adding evidence for a provider or runtime boundary;
- improving diagnostics and actionable error messages;
- proposing a capability with a clear use case and operational model;
- reviewing changes for correctness, compatibility and clarity.

## Report a bug

Open a [GitHub issue](https://github.com/candensa/peren/issues/new) with the smallest reproduction you can safely share.

Include:

- the Peren version or commit;
- operating system and architecture;
- the command or runtime operation involved;
- a reduced configuration with secrets removed;
- the provider and deployment shape, when relevant;
- what you expected;
- what happened;
- relevant logs with credentials, tenant data and private endpoints removed;
- whether the behavior is repeatable;
- any workaround you have confirmed.

State whether the behavior was observed in local development, a single-node deployment, a multi-node fleet or a live provider environment. Those environments have different persistence and failure boundaries.

## Propose a feature

Feature proposals should begin with the user or operator problem. Explain:

- who needs the capability;
- the concrete task they cannot complete today;
- the expected runtime or operator behavior;
- persistence, compatibility and security consequences;
- provider or protocol dependencies;
- important limits or failure cases;
- alternatives you considered.

A proposal does not need a finished design. It does need enough context to determine whether the capability belongs in Peren and which contract owns it.

## Make a change

1. Keep the change focused on one coherent problem.
2. Put behavior in the crate that owns the domain decision.
3. Add or update tests for observable behavior.
4. Update public documentation when a command, configuration field, runtime API, binding, provider, persisted format or operational contract changes.
5. Run the smallest relevant checks while iterating.
6. Run the required repository checks described in [DEVELOPMENT.md](DEVELOPMENT.md) before requesting review.
7. Explain the resulting behavior and verification in the pull request.

Avoid unrelated formatting, renaming or cleanup in the same change. Small diffs are easier to verify when state safety and compatibility matter.

## Pull request description

A pull request should answer:

- What concrete trigger or use case was wrong or missing?
- What does Peren do after this change?
- Which user-visible contract changes?
- Does the change affect persisted state, configuration, CLI output, runtime compatibility, provider behavior, security, peer protocol or rollback?
- What happens during failure or interruption?
- Which checks prove the result?
- What limitation remains?

Include migration and rollback instructions when existing deployments or persisted data may be affected.

## Documentation contributions

Public documentation lives under `docs/public/en-GB`. Product documentation must describe released behavior and use installed-product commands. Keep source-tree build commands, internal test names, release gates and maintainer workflows in `DEVELOPMENT.md` or other contributor material.

Documentation examples must:

- run against the current product;
- state prerequisites and expected results;
- identify important side effects;
- describe relevant limits and failure behavior;
- use dummy values instead of live credentials;
- avoid local absolute paths and private infrastructure details.

## Review standard

Reviewers assess correctness before style. Reviews should consider:

- data safety and recovery;
- tenant and service isolation;
- ownership and stale-writer fencing;
- provider and credential boundaries;
- explicit error behavior;
- runtime and configuration compatibility;
- upgrade and rollback consequences;
- whether tests prove a user-visible contract;
- whether documentation enables the next decision.

Review feedback should identify the concrete risk or unclear behavior. Technical disagreement is welcome; personal attacks and contempt are not.

## Community conduct

Keep project discussions respectful, focused and useful. Harassment, threats, discriminatory language, personal attacks, credential sharing, spam and deliberate disruption are not accepted in issues, pull requests or other Peren community spaces.

Maintainers may edit or remove content, lock discussions or restrict participation when necessary to protect contributors and keep the project usable. If a conduct concern involves sensitive information or a maintainer, do not post details publicly. Use the private contact path described in the repository profile and state that the report concerns community conduct rather than a product vulnerability.

## Licence

By contributing to Peren, you agree that your contribution may be distributed under the repository's [Apache License 2.0](LICENSE).
