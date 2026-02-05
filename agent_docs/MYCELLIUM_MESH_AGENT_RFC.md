RFC: Mycelium Mesh Agent (MMA)

Status: Draft v1
Audience: Underleaf platform engineers implementing the Mycelium Mesh runtime
Scope: Requirements and architecture for the Mycelium Mesh Agent running as a managed subsystem under the Core Underleaf Agent (UA)

⸻

1. Overview

The Mycelium Mesh Agent (MMA) is the runtime communication and capability-binding subsystem of Underleaf. It provides identity-aware connectivity, locality-first routing, and policy-enforced capability bindings across services, containers, native processes, and embedded adapters at the edge.

MMA must operate as a subordinate subsystem of the Core Underleaf Agent (UA).
UA remains the system of record for cluster membership, lifecycle, identity issuance, policy storage, and verified capability semantics.

MMA provides runtime enforcement and connectivity but does not own cluster truth.

⸻

2. Goals

2.1 Functional Goals
	1.	Provide identity-based service-to-service connectivity across nodes.
	2.	Enable capability-based binding between services.
	3.	Enforce local policy and consent constraints at runtime.
	4.	Prefer local and LAN paths before WAN.
	5.	Operate offline with last-known-good state.
	6.	Support containers, native processes, and embedded device adapters.
	7.	Produce audit and telemetry signals for UA.

2.2 Product Goals
	1.	Make edge clusters behave like a local-first “mesh cloud.”
	2.	Allow composable local service graphs.
	3.	Enable explainable allow/deny decisions.
	4.	Provide safe defaults for high-risk capabilities.
	5.	Allow dev-mode flexibility without compromising prod safety.

⸻

3. Non-Goals

The MMA MUST NOT:
	•	Manage cluster membership.
	•	Install or launch applications.
	•	Issue node or service identities.
	•	Store authoritative policy or capability schema truth.
	•	Perform CI/CD orchestration.
	•	Replace the UA distributed KV store.

⸻

4. Terminology
	•	Capability: A semantic interface defined in UCRS.
	•	Provider: A running service advertising a capability.
	•	Binding: A runtime grant connecting a client service to a provider.
	•	Locality: Node/LAN/WAN proximity preference.
	•	Trust Tier: Provider classification from UCRS metadata.
	•	Capability Cache: Verified UCRS snapshot provided by UA.
	•	Policy: Rules stored by UA and evaluated by MMA.

⸻

5. System Context

MMA runs on every Underleaf node as a UA-managed process.
It interacts with:
	•	UA event stream (membership, lifecycle, policy, cache updates)
	•	Local services via SDK, sidecar, or ambient mode
	•	Other MMA instances across nodes for routing
	•	UA telemetry ingestion endpoints

⸻

6. Architecture

6.1 Core Components

6.1.1 Mesh Runtime
Responsible for:
	•	Connection establishment (mTLS/QUIC/TCP)
	•	Routing decisions
	•	Retry/backoff logic
	•	Streaming support
	•	Local service registry

6.1.2 Binding Engine
Responsible for:
	•	Capability binding decisions
	•	Policy evaluation
	•	Grant issuance
	•	Grant expiration/revocation
	•	Constraint enforcement

6.1.3 Discovery Layer
Responsible for:
	•	Consuming UA membership events
	•	Optional LAN discovery (mDNS)
	•	Provider endpoint indexing
	•	Health tracking

6.1.4 Policy Evaluator
Responsible for:
	•	Evaluating UA-provided policy documents
	•	Checking consent state
	•	Applying trust tier thresholds
	•	Producing reason codes and traces

6.1.5 Telemetry & Audit Buffer
Responsible for:
	•	Recording binding decisions
	•	Recording connection stats
	•	Buffering when UA unavailable
	•	Delivering summaries to UA

6.1.6 Adapter Host
Responsible for:
	•	Bridging embedded/native devices into mesh
	•	Representing device as provider service
	•	Enforcing policy at adapter boundary

⸻

7. Operating Modes

7.1 Sidecar Mode (optional v1)

Proxy per service intercepting traffic.

7.2 Ambient Mode (recommended v1)

Node-level interception:
	•	eBPF/tproxy or equivalent
	•	Minimal per-service footprint

7.3 SDK Mode (required)

Client library for:
	•	capability binding requests
	•	discovery queries
	•	telemetry hooks

⸻

8. Capability Binding Requirements

8.1 Binding Resolution

MMA MUST:
	1.	Validate capability against cache.
	2.	Find candidate providers.
	3.	Apply locality rules.
	4.	Apply trust tier rules.
	5.	Evaluate policy.
	6.	Return allow/deny decision.

8.2 Binding Grant

If allowed, MMA MUST:
	•	Issue a binding ID.
	•	Associate constraints.
	•	Track expiration.
	•	Enforce limits.

8.3 Enforcement

MMA MUST enforce:
	•	Rate limits
	•	Locality restrictions
	•	Trust tier requirements
	•	Expiration
	•	Revocation

⸻

9. Routing Requirements

9.1 Locality Preference Order
	1.	Same node
	2.	Same LAN
	3.	Same cluster site
	4.	WAN fallback (if allowed)

9.2 Health Awareness

Routing MUST consider:
	•	latency
	•	failures
	•	health events from UA

9.3 Protocol Support

MMA MUST support:
	•	HTTP/1.1
	•	HTTP/2
	•	gRPC
	•	WebSocket
	•	raw TCP streams

QUIC support SHOULD be implemented for WAN resilience.

⸻

10. Discovery Requirements

10.1 UA Events

MMA MUST derive authoritative membership and service presence from UA events.

10.2 LAN Discovery

MMA MAY use mDNS for:
	•	faster convergence
	•	endpoint hints

But MUST NOT treat it as authoritative.

10.3 Provider Advertisement

A service MAY advertise capabilities only if:
	•	capability exists in cache
	•	service identity is valid
	•	UA lifecycle event confirms running state

⸻

11. Policy Requirements

11.1 Evaluation Inputs
	•	capability risk class
	•	provider trust tier
	•	policy document
	•	consent state
	•	locality constraints
	•	provenance labels

11.2 Default Behaviors

If no explicit policy:
	•	low risk → allow (configurable)
	•	medium risk → allow if trust tier ≥ certified
	•	high risk → deny unless explicitly allowed

11.3 Explainability

Every deny MUST include:
	•	reason code
	•	policy reference
	•	capability id

⸻

12. Identity Requirements

MMA MUST:
	•	Use UA-issued identities for all mTLS.
	•	Reject connections with invalid identities.
	•	Bind session grants to identity.
	•	Support identity rotation events.

⸻

13. Offline Operation

MMA MUST continue functioning when:
	•	UA temporarily unavailable
	•	WAN disconnected
	•	UCRS unreachable

Using:
	•	last policy snapshot
	•	last capability cache
	•	local membership view

⸻

14. Telemetry Requirements

MMA MUST collect:
	•	binding decisions
	•	connection metrics
	•	error rates
	•	deny reasons
	•	latency histograms

MMA MUST buffer locally until UA flush.

⸻

15. Performance Requirements
	•	Binding decision latency ≤ 10ms local
	•	Connection setup overhead ≤ 20ms local
	•	Memory footprint ≤ 100MB target
	•	CPU overhead ≤ 5% typical node

⸻

16. Security Requirements
	•	All control APIs node-local only.
	•	UA authentication required.
	•	Capability cache must be verified.
	•	Provider identity must match provenance labels.
	•	Default deny for unknown capability.

⸻

17. Failure Handling

17.1 UA Down
	•	Continue enforcing
	•	Buffer audit

17.2 Cache Missing
	•	Deny unknown capabilities
	•	Allow existing bindings until expiry

17.3 Policy Missing
	•	Default safe mode (deny medium/high risk)

⸻

18. Configuration Requirements

MMA MUST accept configuration only from UA:
	•	mesh mode
	•	telemetry level
	•	policy reference
	•	cache reference
	•	locality defaults

⸻

19. Upgrade Requirements

UA manages MMA upgrades.
MMA MUST support:
	•	rolling restart
	•	state persistence across restart
	•	backward compatible contract

⸻

20. Observability

MMA MUST expose:
	•	health endpoint
	•	metrics endpoint
	•	binding inspection
	•	mesh summary

All node-local only.

⸻

21. Future Extensions (Non-v1)
	•	Multi-cluster federation routing
	•	Cross-org trust
	•	Capability marketplace signing
	•	Adaptive routing heuristics
	•	WASM policy engine
	•	Intent-based auto-binding

⸻

22. Acceptance Criteria (v1)

MMA v1 is complete when:
	1.	UA can start/stop/configure MMA.
	2.	Services can request capability bindings.
	3.	MMA enforces policy and locality.
	4.	Offline operation works.
	5.	Audit events reach UA.
	6.	Identity-based routing functions across nodes.
	7.	Embedded adapters can participate.
	8.	Deny decisions are explainable.

⸻

23. Summary

Mycelium Mesh is not a replacement for Underleaf’s core agent.
It is the runtime communication substrate that makes an edge cluster behave like a local-first programmable system.

UA defines truth.
MMA enforces runtime behavior.

The separation must remain strict.
