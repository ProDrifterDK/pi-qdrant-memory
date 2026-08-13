import { contentId, observationId, stateKey, curatedCurrentId, validateEffectiveOrder, MAX_SESSION_SEQUENCE, type EffectiveOrder } from "../domain/ids.js";
import type { EpisodeRecord, CuratedMemoryRecord } from "../domain/records.js";
import { canonicalStringify } from "../domain/canonical.js";
import { gateCuratedEgressText } from "../security/egress.js";

export interface ProjectionItem { readonly category: string; readonly scope: string; readonly subject: string; readonly predicate: string; readonly evidence: readonly string[]; readonly value?: unknown; readonly text?: string; readonly confidence?: number; }
export interface CurationProjection {
  readonly stateKey: string; readonly contentId: string; readonly effectiveOrder: EffectiveOrder; readonly primary: EpisodeRecord; readonly evidence: readonly EpisodeRecord[]; readonly observationId: string; readonly currentId: string; readonly text: string; readonly scope: string; readonly projectId?: string;
}
export function primaryEvidence(evidence: readonly EpisodeRecord[]): EpisodeRecord { if (evidence.length === 0) throw new TypeError("Projection evidence is empty"); let primary=evidence[0]!; for (const ep of evidence) { const at=Date.parse(ep.eventAt), prior=Date.parse(primary.eventAt); if (at>prior || (at===prior && ep.id>primary.id)) primary=ep; } return primary; }
export function projectEffectiveOrder(evidence: readonly EpisodeRecord[], value: string): EffectiveOrder { const sessions=new Set(evidence.map((ep)=>ep.sessionId)); if (sessions.size===1 && evidence.every((ep)=>ep.sessionSequence!==undefined)) { const primary=primaryEvidence(evidence); const sequence=Math.max(...evidence.map((ep)=>ep.sessionSequence!)); if (!Number.isSafeInteger(sequence) || sequence < 0 || sequence > MAX_SESSION_SEQUENCE) throw new TypeError("Projection session sequence is invalid"); return Object.freeze({kind:"session",sessionId:primary.sessionId,sequence,eventAt:primary.eventAt,episodeId:primary.id,contentId:value}); } const primary=primaryEvidence(evidence); return [primary.eventAt,primary.id,value] as [string,string,string]; }
export function projectCurationText(item: ProjectionItem): string {
  const valuePart = item.value !== undefined ? canonicalStringify(item.value) : (item.text ?? "");
  return gateCuratedEgressText([`category:${item.category}`,`scope:${item.scope}`,`subject:${item.subject}`,`predicate:${item.predicate}`,`value:${valuePart}`,`evidence:${[...item.evidence].sort().join(",")}`].join("\n"),{maxChars:16000,homeDir:"/"}).text;
}
export function projectCurationItem(host: "pi"|"prime", policyHash: string, policyEpoch: number, item: ProjectionItem, episodes: ReadonlyMap<string, EpisodeRecord>): CurationProjection {
  const evidence=item.evidence.map((id)=>episodes.get(id)); if (evidence.some((ep)=>ep===undefined)) throw new TypeError("Projection evidence is missing"); const present=evidence as EpisodeRecord[]; let projectId:string|undefined; if(item.scope==="project"){const set=new Set(present.map((ep)=>ep.projectId)); if(set.size!==1)throw new TypeError("Project-scoped evidence spans multiple projects"); projectId=[...set][0];} if(item.scope==="session"){const set=new Set(present.map((ep)=>ep.sessionId)); if(set.size!==1)throw new TypeError("Session-scoped evidence spans multiple sessions");}
  const state=stateKey({host,scope:item.scope,...(projectId===undefined?{}:{projectId}),category:item.category,subject:item.subject,predicate:item.predicate}); const value=item.value!==undefined?item.value:item.text; const content=contentId(policyHash,state,value); const order=projectEffectiveOrder(present,content); const primary=primaryEvidence(present); const occurrence=observationId(policyEpoch,content,primary.id,order); const text=projectCurationText(item);
  return Object.freeze({stateKey:state,contentId:content,effectiveOrder:order,primary,evidence:Object.freeze(present),observationId:occurrence,currentId:curatedCurrentId(host,state,policyEpoch),text,scope:item.scope,...(projectId===undefined?{}:{projectId})});
}

export interface ConflictAggregateProjection {
  readonly members: readonly string[];
  readonly representatives: readonly CuratedMemoryRecord[];
  readonly sourceEpisodeIds: readonly string[];
  readonly effectiveOrder: EffectiveOrder;
  readonly createdAt: string;
}
function orderKey(order: EffectiveOrder): string { return canonicalStringify(order); }
function laterDeterministic(left: CuratedMemoryRecord, right: CuratedMemoryRecord): CuratedMemoryRecord {
  const leftSession = !Array.isArray(left.effectiveOrder) && typeof left.effectiveOrder === "object" ? left.effectiveOrder as Extract<EffectiveOrder, { readonly kind: "session" }> : null;
  const rightSession = !Array.isArray(right.effectiveOrder) && typeof right.effectiveOrder === "object" ? right.effectiveOrder as Extract<EffectiveOrder, { readonly kind: "session" }> : null;
  // Same-session causal sequence is authoritative even when wall clocks move
  // backwards. eventAt is only a cross-session/deterministic fallback.
  if (leftSession !== null && rightSession !== null && leftSession.sessionId === rightSession.sessionId && leftSession.sequence !== rightSession.sequence) {
    return leftSession.sequence > rightSession.sequence ? left : right;
  }
  const leftDate=Date.parse(left.eventAt), rightDate=Date.parse(right.eventAt);
  if (leftDate !== rightDate) return leftDate > rightDate ? left : right;
  const leftOrder=orderKey(left.effectiveOrder), rightOrder=orderKey(right.effectiveOrder);
  if (leftOrder !== rightOrder) return leftOrder > rightOrder ? left : right;
  return left.id >= right.id ? left : right;
}
/** Pure aggregate over the complete sorted conflict membership. Arrival order is
 * never consulted: members, logical representatives, source closure, causal
 * order and envelope timestamp all derive from the immutable member set. */
export function projectConflictAggregate(membersInput: readonly CuratedMemoryRecord[]): ConflictAggregateProjection {
  if (!Array.isArray(membersInput) || membersInput.length < 2) throw new TypeError("Conflict aggregate requires at least two members");
  const members = [...membersInput].sort((a,b)=>a.id.localeCompare(b.id));
  if (new Set(members.map((member)=>member.id)).size !== members.length) throw new TypeError("Conflict aggregate members repeat");
  const first=members[0]!;
  if (members.some((member)=>member.stateKey !== first.stateKey || member.ownerHost !== first.ownerHost || member.processingPolicyId !== first.processingPolicyId || member.expiresAt !== first.expiresAt || member.coordinationPolicyHash !== first.coordinationPolicyHash || member.coordinationPolicyEpoch !== first.coordinationPolicyEpoch || member.privacyEpoch !== first.privacyEpoch)) throw new TypeError("Conflict aggregate envelope mismatch");
  const representativeMap = new Map<string, CuratedMemoryRecord>();
  for (const member of members) { const previous=representativeMap.get(member.contentId); if (previous===undefined || laterDeterministic(previous,member)===member) representativeMap.set(member.contentId,member); }
  const representatives=[...representativeMap.values()].sort((a,b)=>a.contentId.localeCompare(b.contentId)||a.id.localeCompare(b.id));
  if (representatives.length < 2) throw new TypeError("Conflict aggregate requires distinct logical content");
  let latest=representatives[0]!;
  for (const member of representatives.slice(1)) latest=laterDeterministic(latest,member);
  // The manifest stores only canonical representatives. Derive every other
  // aggregate field from that same representative set; using raw occurrence
  // members here would make a duplicate logical content affect createdAt or
  // provenance without being reconstructible from manifest.members.
  const canonicalMembers = representatives.map((member) => member.id).sort();
  const sourceEpisodeIds=[...new Set(representatives.flatMap((member)=>member.sourceEpisodeIds ?? member.provenance ?? []))].sort();
  const earliest=[...representatives].sort((a,b)=>Date.parse(a.eventAt)-Date.parse(b.eventAt)||a.id.localeCompare(b.id))[0]!;
  validateEffectiveOrder(latest.effectiveOrder);
  return Object.freeze({members:Object.freeze(canonicalMembers), representatives:Object.freeze(representatives), sourceEpisodeIds:Object.freeze(sourceEpisodeIds), effectiveOrder:latest.effectiveOrder, createdAt:earliest.eventAt});
}

/** Shared OCC order comparator used by materialization and completion. */
export function effectiveOrderTuple(value: EffectiveOrder): readonly [string, string, string] | null {
  if (typeof value === "string") return null;
  if (Array.isArray(value)) return value as readonly [string, string, string];
  const session = value as Extract<EffectiveOrder, { readonly kind: "session" }>;
  return [session.eventAt, session.episodeId, session.contentId];
}
export function compareProjectionOrders(a: EffectiveOrder, b: EffectiveOrder, skew: number): "before" | "after" | "equal" | "within_skew" {
  if (!Number.isSafeInteger(skew) || skew < 0) throw new TypeError("Clock skew is invalid");
  validateEffectiveOrder(a); validateEffectiveOrder(b);
  if (canonicalStringify(a) === canonicalStringify(b)) return "equal";
  const as = !Array.isArray(a) && typeof a === "object" ? a as Extract<EffectiveOrder, { readonly kind: "session" }> : null;
  const bs = !Array.isArray(b) && typeof b === "object" ? b as Extract<EffectiveOrder, { readonly kind: "session" }> : null;
  if (as !== null && bs !== null && as.sessionId === bs.sessionId) {
    if (as.sequence !== bs.sequence) return as.sequence < bs.sequence ? "before" : "after";
    return as.contentId === bs.contentId ? "equal" : "within_skew";
  }
  if (typeof a === "string" || typeof b === "string") return "within_skew";
  const at = Array.isArray(a) ? a[0] : as!.eventAt;
  const bt = Array.isArray(b) ? b[0] : bs!.eventAt;
  const delta = Date.parse(at) - Date.parse(bt);
  if (Math.abs(delta) <= skew) {
    const ac = Array.isArray(a) ? a[2] : as!.contentId;
    const bc = Array.isArray(b) ? b[2] : bs!.contentId;
    return ac === bc ? "equal" : "within_skew";
  }
  if (delta !== 0) return delta < 0 ? "before" : "after";
  const ae = Array.isArray(a) ? a[1] : as!.episodeId;
  const be = Array.isArray(b) ? b[1] : bs!.episodeId;
  if (ae !== be) return ae < be ? "before" : "after";
  const ac = Array.isArray(a) ? a[2] : as!.contentId;
  const bc = Array.isArray(b) ? b[2] : bs!.contentId;
  return ac < bc ? "before" : "after";
}
