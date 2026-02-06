# Quick Reference: Remaining Critical Issues

## 🔴 CRITICAL (Fix Before Production)

### 1. Server Works But Missing UA Integration
**File**: `internal/discovery/ua_event_consumer.go:72`  
**Line**: `// TODO: Establish gRPC connection to UA event stream`  
**Impact**: No actual event consumption - registry never updates  
**Fix Priority**: 🔥 HIGHEST

**What needs doing**:
```go
// Current (BROKEN):
func (ec *EventConsumer) Start(ctx context.Context) error {
    // TODO: Establish gRPC connection to UA event stream
    return nil
}

// Need to implement:
- gRPC dial to UA control channel
- Stream UAEvent messages
- Handle reconnection
- Process events with handleXXX methods
```

---

### 2. Policy Evaluator Doesn't Use Provider Info
**File**: `internal/binding_engine/engine.go:119`  
**Impact**: Evaluates same request for all providers - can't differentiate  
**Fix Priority**: 🔥 HIGH

**Current Code** (INCORRECT):
```go
for _, provider := range providers {
    // BUG: Evaluates same request for all providers
    decision, reason, err := e.policyEvaluator.EvaluateBinding(ctx, req)
    // Provider-specific context not passed!
}
```

**Solution**:
```go
// Option 1: Add provider to interface
decision, reason, err := e.policyEvaluator.EvaluateBinding(ctx, req, provider)

// Option 2: Create eval context
evalCtx := &types.EvaluationContext{
    Request: req,
    Provider: provider,
    Timestamp: time.Now(),
}
decision, reason, err := e.policyEvaluator.Evaluate(ctx, evalCtx)
```

---

### 3. Race Condition in EventConsumer Start/Stop
**File**: `internal/discovery/ua_event_consumer.go:64-68`  
**Impact**: Concurrent Start() calls can corrupt state  
**Fix Priority**: 🔥 HIGH

**Current Code** (RACE):
```go
func (ec *EventConsumer) Start(ctx context.Context) error {
    ec.mu.Lock()
    if ec.running {
        ec.mu.Unlock()  // UNLOCK TOO EARLY
        return errors.New("event consumer already running")
    }
    ec.mu.Unlock()

    ec.running = true  // RACE: Not holding lock!
    // ...
}
```

**Fixed Code**:
```go
func (ec *EventConsumer) Start(ctx context.Context) error {
    ec.mu.Lock()
    defer ec.mu.Unlock()  // Hold lock for entire operation
    
    if ec.running {
        return errors.New("event consumer already running")
    }
    
    ec.running = true  // Safe now
    // ...rest of function with lock held
}
```

---

## ⚠️ MAJOR (Fix Soon)

### 4. No Input Validation on API Handlers
**File**: `internal/server/server.go` - all handlers  
**Impact**: Security vulnerability, crashes on bad input  
**Fix Priority**: 🟠 MEDIUM-HIGH

**Example** - handleBindingRequest needs:
```go
func (s *Server) handleBindingRequest(c *gin.Context) {
    var req types.BindingRequest
    if err := c.ShouldBindJSON(&req); err != nil {
        c.JSON(400, gin.H{"error": "invalid request"})
        return
    }
    
    // ADD THIS VALIDATION:
    if req.Client.ServiceID == "" {
        c.JSON(400, gin.H{"error": "client.service_id required"})
        return
    }
    if req.Capability.ID == "" {
        c.JSON(400, gin.H{"error": "capability.id required"})
        return
    }
    if len(req.Client.ServiceID) > 256 {
        c.JSON(400, gin.H{"error": "service_id too long"})
        return
    }
    // ... validate all fields
    
    // Then process
    resp, err := s.bindingEngine.RequestBinding(c.Request.Context(), &req)
    // ...
}
```

---

### 5. Grant Operations Don't Check Errors
**File**: `internal/binding_engine/grants.go`  
**Impact**: Silent failures, potential nil pointer crashes  
**Fix Priority**: 🟠 MEDIUM

**Current Code** (NO ERROR HANDLING):
```go
func (gm *GrantManager) GetGrant(bindingID string) *types.BindingGrant {
    gm.mu.RLock()
    defer gm.mu.RUnlock()
    return gm.activeGrants[bindingID]  // Could be nil!
}
```

**Need to Add**:
- Validate bindingID not empty
- Return error for not found
- Check grant not expired before returning

---

### 6. TODO Placeholders Block Features

**Grant Renewal** - `internal/binding_engine/engine.go:148`:
```go
// TODO: Implement actual grant renewal based on usage
return true, nil
```
**Impact**: Grants never actually renew - will expire after 10s

**UA Telemetry Delivery** - `internal/telemetry/flusher.go:97`:
```go
// TODO: Send to UA via gRPC
return nil
```
**Impact**: Telemetry never delivered to UA

**Rate Limiting** - `internal/policy_evaluator/evaluator.go:91`:
```go
// TODO: Implement actual rate limiting per client/capability
return true, "", nil
```
**Impact**: No protection against binding spam

**Config Reload** - `cmd/serve/main.go:220`:
```go
case syscall.SIGHUP:
    logger.Info("received SIGHUP - reloading config")
    // TODO: Implement config reload
```
**Impact**: Can't reload config without restart

---

## 🟡 DESIGN ISSUES

### 7. Capability Cache Not Thread-Safe
**File**: `internal/types/mesh.go` - CapabilityCache  
**Current**: Plain `map[string]*Capability` - no locking  
**Solution**: Wrap in struct with `sync.RWMutex`

### 8. Telemetry Buffer Unbounded
**File**: `internal/telemetry/buffer.go`  
**Current**: `events []AuditEvent` grows forever  
**Solution**: Implement ring buffer or bounded queue

### 9. No Context Cancellation Checks
**Files**: `discovery/ua_event_consumer.go`, `binding_engine/engine.go`  
**Issue**: Long-running loops ignore context cancellation  
**Solution**: Add `select { case <-ctx.Done(): return }`

---

## 🔧 Quick Fixes (< 30 minutes each)

### Hardcoded Magic Numbers
```go
// binding_engine/engine.go:109
ExpiresAt: time.Now().Add(10 * time.Second)  // Magic number!

// Should be:
ExpiresAt: time.Now().Add(cfg.DefaultGrantDuration)
```

### Copy-Paste Error Messages
Multiple places have `errors.New("...")` - should use sentinel errors from `types/errors.go`

### Inconsistent Naming
- `ClientID` vs `ClientServiceID` (fixed in GetBinding)
- `ProviderID` vs `ProviderServiceID` (fixed in GetBinding)
- `RevokedReason` vs `LastError` (fixed in GetBinding)

---

## 📊 Priority Matrix

| Issue | Priority | Effort | Impact if Unfixed |
|-------|----------|--------|-------------------|
| UA gRPC Connection | 🔥🔥🔥 | 1 day | Core feature broken |
| Policy Provider Context | 🔥🔥 | 2 hours | Wrong policy decisions |
| EventConsumer Race | 🔥🔥 | 1 hour | Crashes on concurrent use |
| Input Validation | 🔥 | 4 hours | Security vulnerability |
| Grant Error Handling | 🔥 | 2 hours | Silent failures |
| TODO Placeholders | 🟠 | 1 week | Features incomplete |
| Thread Safety | 🟠 | 1 day | Rare race conditions |
| Context Cancellation | 🟡 | 2 hours | Resource leaks |

---

## 🎯 Suggested Fix Order

### Week 1: Make It Work
1. ✅ ~~Fix runtime lifecycle~~ (DONE)
2. ⚠️ Fix policy evaluation provider context (2h)
3. ⚠️ Fix EventConsumer race condition (1h)
4. ⚠️ Add input validation to API handlers (4h)
5. ⚠️ Implement UA gRPC connection (1-2 days)

### Week 2: Make It Right
6. Grant error handling (2h)
7. Implement grant renewal logic (4h)
8. Add UA telemetry delivery (4h)
9. Implement rate limiting (4h)
10. Fix thread safety issues (1 day)

### Week 3: Make It Fast
11. Add context cancellation checks (2h)
12. Implement bounded telemetry buffer (4h)
13. Add connection pooling (1 day)
14. Optimize registry lookups (4h)

### Week 4: Make It Solid
15. Add comprehensive tests (1 week)
16. Security audit (2 days)
17. Performance testing (2 days)
18. Documentation (ongoing)

---

## 🚀 Ready to Deploy Checklist

- [x] Compiles
- [x] Runs  
- [x] API responds
- [x] Graceful shutdown
- [ ] UA integration works
- [ ] Input validation complete
- [ ] No race conditions
- [ ] Error handling robust
- [ ] mTLS configured
- [ ] Tests passing (>80% coverage)
- [ ] Observability working
- [ ] Security hardened
- [ ] Load tested
- [ ] Documentation complete

**Current Status**: 4/14 complete (29%)  
**Estimated Time to Production**: 4-6 weeks

---

## 📞 Need Help?

- Review full details: See [SENIOR_REVIEW_SUMMARY.md](SENIOR_REVIEW_SUMMARY.md)
- See what was fixed: See [FIXES_APPLIED.md](FIXES_APPLIED.md)  
- See all issues: See [CODE_REVIEW.md](CODE_REVIEW.md)
- Test server: Run `./test_server.sh`
- Check logs: `cat mma.log | jq`

---

**Last Updated**: 2026-02-04  
**Status**: ✅ Phase 1 Complete, ⚠️ Phase 2 Ready to Start
