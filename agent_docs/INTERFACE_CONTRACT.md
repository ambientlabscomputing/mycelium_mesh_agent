Subsystem Contract: Underleaf Agent (UA) ↔ Mycelium Mesh Agent (MMA)

0. Purpose

This contract defines the only allowed integration surface between the Core Underleaf Agent (UA) and the Mycelium Mesh Agent (MMA). It ensures MMA builds on UA as a managed subsystem and does not intrude into UA-owned domains (cluster truth, lifecycle, backend auth).

⸻

1) Roles & Authority

1.1 UA is the System of Record for
	•	Cluster membership & health
	•	Distributed KV store contents and replication
	•	Node/service identity issuance and trust roots
	•	App/service install/start/stop/update orchestration
	•	Policy documents & consent state storage
	•	UCRS snapshot verification and caching (Local Capability Cache)

1.2 MMA is the System of Record for
	•	Runtime data-plane channels (mTLS sessions, routing, retries)
	•	Runtime capability bindings (grant issuance, expiration, enforcement)
	•	Mesh-level discovery cache (derived from UA events + LAN discovery)
	•	Local telemetry buffers generated from mesh activity

1.3 Fundamental Control Relationship
	•	UA manages MMA lifecycle (install, configure, start/stop, upgrade).
	•	MMA consumes UA-issued identity/membership/lifecycle/policy/capability cache.
	•	MMA cannot mutate UA-owned state directly.

⸻

2) Transport & Security Assumptions

2.1 Control Channel

UA↔MMA control plane communication occurs over a node-local control channel:
	•	Unix domain socket preferred (Linux), loopback TCP allowed for portability.
	•	All requests MUST be authenticated using UA-issued node identity.
	•	MMA MUST reject control requests not authenticated as UA.

2.2 Data/Policy Inputs
	•	MMA MUST treat UCRS cache and policy documents as read-only inputs from UA.
	•	MMA MUST NOT fetch UCRS directly in v1 (optional future enhancement with explicit UA approval).

⸻

3) UA → MMA Event Stream

UA publishes a strictly defined stream of events that MMA uses to build the mesh registry and routing tables. Events are append-only, idempotent, and monotonic per entity version.

3.1 Event Envelope

All UA→MMA events MUST use this envelope:
	•	event_id: globally unique (UUID/ULID)
	•	event_type: string
	•	emitted_at: RFC3339 timestamp
	•	cluster_id: string
	•	node_id: string (origin)
	•	seq: monotonically increasing per origin node (for ordering)
	•	entity_ref: { kind, id } (optional)
	•	payload: object
	•	signature: optional (recommended) UA signature over envelope+payload

3.2 Required Events

A) Cluster & Membership
	1.	cluster.snapshot
	•	Emitted on MMA start and periodically (or on demand).
	•	Payload:
	•	members[]: { node_id, node_identity, endpoints[], tags{}, status }
	•	cluster_ca_fingerprint
	•	mesh_config_version
	2.	member.joined
	•	Payload: { node_id, node_identity, endpoints[], tags{} }
	3.	member.updated
	•	Payload: { node_id, endpoints[]?, tags{}?, status? }
	4.	member.left
	•	Payload: { node_id, reason }
	5.	member.health
	•	Payload: { node_id, status: healthy|degraded|unreachable, last_seen }

Note: MMA may run LAN discovery (mDNS), but membership truth is UA events. mDNS results are treated only as hints unless corroborated by UA.

B) Identity & Trust
	6.	identity.trust_roots.updated
	•	Payload:
	•	cluster_trust_bundle (or reference)
	•	valid_from, valid_to
	•	rotation_id
	7.	identity.service.issued
	•	Payload:
	•	service_id
	•	spiffe_like_id (or Underleaf identity URI)
	•	cert_ref (or inline cert chain)
	•	claims (provider provenance labels, org/app ids)
	•	expires_at
	8.	identity.service.revoked
	•	Payload: { service_id, reason, revoked_at }

C) Service Lifecycle (Mesh-Relevant)
	9.	service.started
	•	Payload:
	•	service_id
	•	service_identity
	•	node_id
	•	endpoints[]: { proto, host, port, scope: node|lan|wan, tags{} }
	•	capabilities_provided[]: { capability_id, version } (optional; validated against UA cache)
	•	labels{} (for routing/policy: app_id, env, role, etc.)
	10.	service.updated

	•	Payload:
	•	service_id
	•	endpoints[]?
	•	capabilities_provided[]?
	•	labels{}?
	•	rolling_state? (optional hint: canary/blue-green stage)

	11.	service.stopped

	•	Payload: { service_id, reason }

UA remains the only source for whether a service is running. MMA MUST NOT infer running state solely from traffic.

D) Capability Semantics Cache (UCRS-derived)
	12.	capability_cache.snapshot.updated

	•	Payload:
	•	cache_version
	•	signed_snapshot_ref (or inline)
	•	verified_at
	•	schema_index_digest (hash of normalized schema set)

	13.	capability_cache.delta.updated

	•	Payload:
	•	cache_version
	•	delta_ref
	•	verified_at
	•	schema_index_digest

MMA MUST validate that any advertised capability exists in schema_index_digest before accepting it into the mesh registry.

E) Policy & Consent Inputs
	14.	mesh_policy.updated

	•	Payload:
	•	policy_version
	•	policy_ref (or inline)
	•	effective_at

	15.	consent_state.updated

	•	Payload:
	•	subject_ref (user/app/service)
	•	capability_id
	•	decision: allow|deny
	•	constraints{} (ttl, locality, scopes)
	•	version

⸻

4) MMA → UA Minimal APIs

MMA exposes a small node-local API for UA to:
	•	configure MMA
	•	request introspection
	•	receive audit/telemetry summaries
	•	(optionally) query binding state

4.1 API Conventions
	•	All endpoints are node-local only.
	•	UA authenticates using node identity; MMA authorizes UA with an allowlist of UA certs/keys.
	•	APIs are versioned: /v1/...
	•	MMA MUST be able to operate if UA is temporarily unavailable, but MUST buffer and later deliver required reports (audit summaries).

⸻

5) MMA APIs (v1)

5.1 POST /v1/bindings/request

Request a runtime capability binding for a client service.

Request
	•	request_id: string (idempotency)
	•	client: { service_id, identity }
	•	capability: { id, version_constraint? }
	•	constraints:
	•	locality: node_only|lan_preferred|lan_only|wan_allowed
	•	max_latency_ms?
	•	min_trust_tier? (official/certified/community/experimental/local)
	•	require_digest_match?: boolean
	•	ttl_ms?
	•	rate_limit?: { rps?, burst? }
	•	context:
	•	user_present? boolean (for consent prompts)
	•	purpose? string (audit)

Response
	•	decision: allow|deny
	•	reason_code: enum (see below)
	•	if allow:
	•	binding_id
	•	provider: { service_id, identity, endpoint }
	•	grant: { token_ref? | mTLS_context_ref?, expires_at }
	•	enforced_constraints (final resolved)

Reason Codes (minimum)
	•	CAPABILITY_UNKNOWN (not in UA cache)
	•	NO_PROVIDER_AVAILABLE
	•	POLICY_DENIED
	•	CONSENT_REQUIRED
	•	TRUST_TIER_INSUFFICIENT
	•	PROVENANCE_MISMATCH
	•	LOCALITY_VIOLATION
	•	RATE_LIMITED
	•	INTERNAL_ERROR

Notes:
	•	MMA consults UA-provided capability cache and policy inputs; it does not fetch global state.
	•	“Consent required” should trigger UA/UI flow via UA systems; MMA only signals requirement.

⸻

5.2 GET /v1/bindings/{binding_id}

Return current status of a binding grant.

Response
	•	binding_id
	•	state: active|expired|revoked|failed
	•	client_service_id
	•	provider_service_id
	•	capability_id
	•	created_at, expires_at
	•	enforced_constraints
	•	last_error?

⸻

5.3 POST /v1/introspect/service

Returns mesh-relevant view for a service identity (for debugging/UI).

Request
	•	service_id or identity
	•	include_routes? boolean
	•	include_policies? boolean (returns references/reasoning, not raw secrets)

Response
	•	service: { service_id, identity, node_id, endpoints[], labels{} }
	•	provides[] capabilities (validated)
	•	consumes[] active bindings
	•	routes[] (optional): resolved peers/locality info
	•	policy_trace_ref? (optional pointer for “explain” endpoint)

⸻

5.4 GET /v1/introspect/mesh

Mesh health and summary.

Response
	•	mesh_state: ready|degraded|offline
	•	known_members: count
	•	known_services: count
	•	capability_cache_version
	•	policy_version
	•	data_plane: { mode: sidecar|ambient, status }
	•	buffers: { telemetry_bytes, audit_events_queued }

⸻

5.5 POST /v1/telemetry/flush

UA requests MMA to flush buffered telemetry/audit summaries to UA (or returns them inline).

Request
	•	types: [audit|metrics|traces|logs]
	•	since? timestamp
	•	max_bytes?

Response
	•	delivered: { audit_count, metric_points, trace_spans, log_lines }
	•	remaining_queued
	•	delivery_ref? (if stored in UA KV or file)

⸻

5.6 POST /v1/config/apply

UA applies configuration to MMA (generated from cluster policy/intent).

Request
	•	config_version
	•	mesh_mode: sidecar|ambient
	•	locality_defaults
	•	telemetry_defaults
	•	policy_ref (points to UA policy storage)
	•	capability_cache_ref (points to UA cache storage)

Response
	•	applied: boolean
	•	effective_config_version
	•	warnings[]

MMA MUST NOT accept config changes from any caller except UA.

⸻

6) MMA → UA Event Emission (Audit & Signals)

MMA emits events back to UA (either pushed to UA endpoint or buffered for UA polling via /telemetry/flush).

6.1 Event Types
	1.	mesh.binding.granted
	•	Payload: { binding_id, client_service_id, provider_service_id, capability_id, expires_at, constraints }
	2.	mesh.binding.denied
	•	Payload: { request_id, client_service_id, capability_id, reason_code, policy_version, cache_version }
	3.	mesh.binding.revoked
	•	Payload: { binding_id, reason, revoked_at }
	4.	mesh.policy.decision
	•	Payload: { decision_id, client, provider?, capability_id, decision, reason_code, trace }
	•	(trace is structured and redactable; never includes secrets)
	5.	mesh.telemetry.summary
	•	Payload: { interval, counts, error_rates, top_denies[], top_caps[], buffer_stats }
	6.	mesh.health.changed
	•	Payload: { new_state, reason, observed_at }

⸻

7) Invariants (Non-Negotiable)

7.1 Authority Invariants
	1.	Membership truth is UA-only.
MMA MUST NOT add/remove members or treat mDNS discovery as authoritative membership.
	2.	Lifecycle truth is UA-only.
MMA MUST NOT start/stop/update services or install artifacts.
	3.	Identity issuance is UA-only.
MMA MUST NOT generate node/service identities beyond ephemeral session keys bound to UA-issued identities.
	4.	Policy system-of-record is UA-only.
MMA MAY evaluate and enforce, but policy documents and consent state MUST be persisted in UA-owned storage.
	5.	UCRS verification is UA-only (v1).
MMA MUST consume verified cache references; it MUST NOT accept unsigned/unverified capability schemas.

7.2 Data Flow Invariants
	6.	MMA decisions must be explainable.
Every deny MUST return a reason code; optionally a policy trace reference.
	7.	MMA must operate offline.
With last-known-good cache + policy, MMA continues to bind/enforce locally.
	8.	No hidden coupling.
MMA accesses UA state only through:
	•	UA event stream
	•	UA-provided refs to cache/policy
	•	explicit UA→MMA config endpoint

7.3 Security Invariants
	9.	UA is the only privileged controller.
MMA control APIs MUST authenticate UA; all other callers are denied.
	10.	Capabilities must be schema-valid.
MMA MUST reject provider advertisements for unknown capabilities.
	11.	Provider provenance is enforced for high-risk.
If policy requires digest match, MMA MUST deny on mismatch.

⸻

8) Versioning & Compatibility
	•	Contract versions: ua_mma_contract_version = 1
	•	UA MUST provide cluster.snapshot on MMA startup for fast convergence.
	•	Events are forward-compatible: unknown fields ignored; unknown event types ignored but logged.
	•	MMA MUST surface mesh_state=degraded if required inputs (policy/cache) are missing.

⸻

9) Failure Modes (Defined Behaviors)
	1.	UA unreachable (temporarily)
	•	MMA continues enforcing with last-known policy/cache.
	•	MMA buffers audit/telemetry until UA returns.
	2.	Cache missing/unverified
	•	MMA denies bindings for capabilities not in cache (CAPABILITY_UNKNOWN).
	•	Existing bindings may continue until expiry (policy-dependent; default: continue).
	3.	Policy missing
	•	MMA falls back to a default-deny for medium/high risk capabilities.
	•	Low-risk capabilities may use a conservative default-allow list (optional).
