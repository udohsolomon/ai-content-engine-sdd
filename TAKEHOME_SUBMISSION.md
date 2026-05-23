# AI Content-Production Engine — Design Note

This is how I would stabilise and extend the in-flight brand-identity-preserving short-form video engine. I’m assuming the key business risk is not “can we call the providers?” but whether the system can preserve identity, control cost, recover from partial failures, and give operators enough trust to run hundreds of clips/day without eyeballing every output.

## 1. Failure modes under real volume

### The first break will be input quality, not provider rate limits

Short-form source clips are messy: intro overlays, watermarks, motion blur, fast cuts, captions, low bitrate downloads, faces mid-blink, and source URLs disappearing after selection. If bad inputs enter paid generation, every downstream stage gets expensive and noisy.

**Mitigation**
- Copy source clips into R2 at intake time, not batch time. Treat Apify as ingestion, not runtime dependency.
- Persist source checksums, dimensions, duration, codec, source account, Apify run id, and intake status.
- Preflight every selected clip before paid generation: file integrity, aspect ratio, blur, black frames, text/watermark density, face/person visibility where relevant, duplicate/perceptual hash.
- Reject or surface weak inputs before any provider spend.

### First-frame extraction can quietly poison the whole chain

Using frame 0 is brittle. Reels/TikToks often start on a transition, title card, blink, mouth-open frame, or compression artifact. The image provider then regenerates from a weak anchor and the motion provider amplifies it.

**Mitigation**
- Extract and score multiple candidate anchor frames: early frame window, scene-change frames, and a few percentage offsets.
- Score for sharpness, face visibility, eyes-open/landmarks, occlusion, text density, and composition.
- Persist `anchor_frame_idx`, scores, and runner-up frames so an operator can override without rerunning intake.

### Fallback providers are not interchangeable

Nano Banana Pro, Flux Kontext Max, and Seedream v4.5 will not preserve identity in the same way. A fallback can be technically successful but visually wrong. Under load, this creates batches with mixed identity fidelity.

**Mitigation**
- Make fallback policy per brand tier, not global.
  - Tier 1: stall and surface if primary degrades.
  - Tier 2: allow only approved fallback providers.
  - Tier 3: full chain allowed.
- Keep provider-specific prompt templates and negative prompts.
- Persist the provider route actually used on every attempt.
- Require stricter QA/manual review for fallback outputs until enough brand/provider quality data exists.

### Motion generation amplifies small identity errors

A first frame can look acceptable while the motion output morphs the face, changes age cues, distorts hands, loses jewellery/clothing details, or drifts over the first few seconds.

**Mitigation**
- Sample generated video frames at fixed timestamps.
- Compare sampled frames against the approved identity bundle and against the generated anchor frame.
- Watch temporal identity variance, face count changes, crop shifts, black frames, safety flags, and SSIM between anchor frame and video frame 0.
- If motion fails but the anchor image passes, retry only motion. Do not regenerate the image unless the image failed.

### Async provider lifecycle causes double spend and stuck jobs

The dangerous failure is worker crash after dispatch but before persistence, or webhook success never arriving. A naive queue will redeliver, re-dispatch, and charge twice.

**Mitigation**
- Every external provider call creates an append-only `batch_item_attempt` row before waiting for the provider.
- Send an idempotency key when providers support it: hash of clip, identity version, stage, provider, resolved prompt, and attempt number.
- Webhooks go into a `provider_event_inbox` table first; business logic runs asynchronously after commit.
- A reconciler polls provider job IDs for attempts stuck in `dispatched/running` beyond a grace period.
- State machine only moves forward; late `running` events cannot overwrite `succeeded`.

### Cost and credit exhaustion create retry storms

Provider 402/credit exhaustion should never be treated like a transient 5xx. Otherwise workers hot-retry and burn queue capacity while producing nothing.

**Mitigation**
- Estimate batch cost upfront and enforce a batch cost cap.
- Classify 402/credit errors as provider circuit-breakers.
- Pause affected batches, alert operator/admin, and resume only after balance recovers.
- Track cost per approved output, not just cost per generated output.

## 2. Batch controls

I would model a batch as a persisted execution plan, not a loose group of queue jobs.

### Core state to persist

```text
batch
  id, operator_id, selected_source_clip_ids, selected_brand_ids,
  pinned_identity_version_ids, pinned_prompt_version_ids,
  provider_chain_id, status, cost_cap, cost_spent, created_at, completed_at

batch_item
  id, batch_id, clip_id, brand_id, identity_version_id,
  status, current_stage, attempt_count, last_error_class,
  review_required_reason, final_output_key

batch_item_attempt
  id, batch_item_id, stage, provider, provider_job_id,
  status, prompt_version_id, resolved_prompt_hash,
  input_artifact_keys, output_artifact_key,
  identity_score, error_class, raw_error, cost, started_at, finished_at

provider_event_inbox
  id, provider, event_id unique, signature_verified,
  payload, received_at, consumed_at

operator_action
  id, operator_id, target_type, target_id, action, payload, created_at
```

### Operator experience

The operator should be able to:
- select a pool of clips and multiple brand identities;
- see the expanded matrix of `clips × brands` before starting;
- view estimated cost and provider policy before commit;
- pause/resume/cancel a batch;
- retry failed transient items in bulk;
- retry a single stage for one item;
- skip bad inputs;
- approve/reject surfaced outputs;
- see failures grouped by cause, not just a flat failed list.

Live status should be stage-based: preflight, anchor selection, image generation, motion generation, QA, review, complete.

### Retry / skip / surface policy

| Error class | Default action |
|---|---|
| Network/timeout/5xx | Auto-retry with exponential backoff, capped per stage. |
| 429 with Retry-After | Respect provider delay; do not count as quality failure. |
| Credit/402 | Pause provider or batch; alert; no auto-retry loop. |
| Invalid source/input | Try alternate anchor frame once, then surface or skip depending batch policy. |
| Corrupt provider output | Retry same provider once; then fallback if policy allows. |
| Safety/moderation | Surface unless a brand policy explicitly allows provider fallback. |
| Identity low score | Surface. Blind retries can burn money and still drift. |
| Unknown | Surface. Unknown errors need classification before automation. |

### Resumability

On restart, the system reads persisted state:
- If an item is `running` and has a `provider_job_id`, poll the provider and reconcile.
- If it is `running` without a `provider_job_id`, mark the attempt abandoned and re-dispatch safely.
- If an output exists in R2 but the DB transition did not complete, validate the artifact and advance state.
- If the batch was paused, do not dispatch new work; only reconcile already-dispatched attempts.

Prompt versions and identity versions are pinned at batch start. Resuming a batch must not silently pick up newer prompts or edited identity references.

## 3. Prompt versioning, brand preservation, and drift detection

### Prompt versioning

Prompts should be immutable rows, not strings in code.

```text
prompt_template
  id, stage, provider, logical_name

prompt_version
  id, template_id, body, variables_schema, negative_prompt,
  provider_params, parent_version_id, status, changelog, created_at

prompt_assignment
  id, stage, provider, brand_id nullable, active_prompt_version_id
```

Every generation attempt stores the resolved prompt payload/hash after variable substitution, plus provider, model, params, seed where available, identity version, source artifact, and prompt version. Rollback is a pointer flip for future runs; historical runs remain comparable.

### Brand identity preservation

Brand identity should be a versioned bundle:
- approved reference images/video frames;
- reference embeddings;
- provider-specific conditioning assets/settings;
- brand descriptors and forbidden changes;
- identity threshold;
- publish/retire metadata.

Evaluation points:
1. source anchor frame vs identity bundle;
2. regenerated anchor frame vs identity bundle;
3. sampled motion frames vs identity bundle;
4. sampled motion frames vs regenerated anchor frame.

Useful signals:
- face/person embedding similarity;
- CLIP-style semantic similarity against brand descriptors;
- SSIM/perceptual distance for anchor-to-video-frame-0;
- face count and face visibility stability;
- OCR/logo/style checks if relevant;
- operator approval/rejection labels to calibrate thresholds.

I would not trust one score. Identity preservation is partly subjective, so the system should use automated gates for obvious pass/fail and surface the ambiguous middle.

### Silent drift detection

Silent drift is more dangerous than hard failure because the system keeps producing outputs while quality slowly worsens.

Watch:
- identity score rolling mean by brand/provider/prompt version;
- score distribution shift, not just mean shift;
- operator rejection rate and manual-review rate;
- fallback usage rate;
- cost per approved output;
- motion start-frame fidelity;
- safety/moderation flags;
- source preflight rejection rate by source account;
- provider latency and failure-class trends.

I would maintain a golden canary set: fixed source clips × fixed identity versions × frozen prompt versions. Run it nightly and on any prompt/provider-parameter change. If the canary moves without a code or prompt release, assume provider behaviour changed.

## 4. Three questions before starting

1. **What is worse for the business: delayed-but-on-brand or on-time-but-slightly-off-brand?**  
   This decides fallback policy, tiering, concurrency, and when the system should stall versus ship.

2. **What is the rights model for source clips and identity references?**  
   I need to know whether identity data can be sent to all providers, whether usage windows expire, and what audit trail is required for transformed outputs.

3. **What is the operator SLA and expected batch size?**  
   First usable output latency and full-batch completion latency determine queue topology, provider concurrency, retry budgets, cost caps, and UI expectations.
