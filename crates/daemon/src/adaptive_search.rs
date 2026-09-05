use std::collections::{HashMap, HashSet};

use nomai_core::{AffinityHit, EntryMemorySignal};
use serde_json::{Value, json};
use ulid::Ulid;

struct RankedCandidate {
    entry_id: Ulid,
    item: Value,
    fusion_score: f64,
}

fn entry_id(item: &Value) -> Option<Ulid> {
    item["entry"]["id"].as_str()?.parse().ok()
}

/// Apply live affinity, memory, and transient signals to cached base hybrid
/// candidates. Supplemental candidates are admitted only when backed by an
/// affinity and never consume more than `supplement_limit` slots.
pub(crate) fn rank_candidates(
    base: Vec<Value>,
    supplemental: Vec<Value>,
    affinities: Vec<AffinityHit>,
    signals: HashMap<Ulid, EntryMemorySignal>,
    transient: HashSet<Ulid>,
    limit: usize,
    supplement_limit: usize,
) -> Vec<Value> {
    let mut affinity_by_entry = HashMap::<Ulid, AffinityHit>::new();
    for hit in affinities {
        affinity_by_entry
            .entry(hit.entry_id)
            .and_modify(|current| {
                if hit.affinity_score > current.affinity_score {
                    *current = hit.clone();
                }
            })
            .or_insert(hit);
    }

    let mut candidates = HashMap::<Ulid, RankedCandidate>::new();
    for item in base {
        let Some(id) = entry_id(&item) else {
            continue;
        };
        let fusion_score = item["fusion_score"].as_f64().unwrap_or(0.0);
        candidates.entry(id).or_insert(RankedCandidate {
            entry_id: id,
            item,
            fusion_score,
        });
    }

    let mut supplements_added = 0;
    for mut item in supplemental {
        if supplements_added == supplement_limit {
            break;
        }
        let Some(id) = entry_id(&item) else {
            continue;
        };
        if candidates.contains_key(&id) || !affinity_by_entry.contains_key(&id) {
            continue;
        }
        item["fusion_score"] = json!(0.0);
        candidates.insert(
            id,
            RankedCandidate {
                entry_id: id,
                item,
                fusion_score: 0.0,
            },
        );
        supplements_added += 1;
    }

    let mut ranked = candidates
        .into_values()
        .map(|mut candidate| {
            let signal = signals.get(&candidate.entry_id);
            let affinity = affinity_by_entry.get(&candidate.entry_id);
            let affinity_score = affinity.map_or(0.0, |hit| hit.affinity_score);
            let memory_factor = signal.map_or(1.0, |signal| signal.memory_factor);
            let transient_factor = if transient.contains(&candidate.entry_id) {
                0.5
            } else {
                1.0
            };
            let final_score =
                (candidate.fusion_score + affinity_score) * memory_factor * transient_factor;

            candidate.item["score"] = json!(final_score);
            candidate.item["affinity_score"] = json!(affinity_score);
            candidate.item["memory_factor"] = json!(memory_factor);
            candidate.item["signals"] = json!({
                "reinforcement_count": signal.map_or(0, |signal| signal.reinforcement_count),
                "last_reinforced_at": signal.map(|signal| signal.last_reinforced_at),
                "affinity_matched": affinity.is_some(),
            });
            candidate
        })
        .collect::<Vec<_>>();

    ranked.sort_by(|left, right| {
        let left_score = left.item["score"].as_f64().unwrap_or(0.0);
        let right_score = right.item["score"].as_f64().unwrap_or(0.0);
        right_score
            .total_cmp(&left_score)
            .then_with(|| right.fusion_score.total_cmp(&left.fusion_score))
            .then_with(|| left.entry_id.cmp(&right.entry_id))
    });
    ranked.truncate(limit);
    ranked.into_iter().map(|candidate| candidate.item).collect()
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use nomai_core::{AffinityHit, EntryMemorySignal};
    use serde_json::{Value, json};
    use ulid::Ulid;

    use super::rank_candidates;

    fn candidate(id: Ulid, fusion_score: f64) -> Value {
        json!({"entry":{"id":id.to_string()}, "fusion_score":fusion_score})
    }

    fn signal(id: Ulid, factor: f64) -> EntryMemorySignal {
        EntryMemorySignal {
            entry_id: id,
            reinforcement_count: 0,
            last_reinforced_at: "2026-01-01T00:00:00Z".parse().unwrap(),
            memory_factor: factor,
        }
    }

    fn affinity(id: Ulid, rank: u32, score: f64) -> AffinityHit {
        AffinityHit {
            entry_id: id,
            block_id: None,
            chunk_id: None,
            similarity: 1.0,
            confidence: 1.0,
            affinity_rank: rank,
            affinity_score: score,
        }
    }

    #[test]
    fn old_unreinforced_entry_is_softly_downranked_but_retained() {
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let ranked = rank_candidates(
            vec![candidate(id, 0.02)],
            vec![],
            vec![],
            HashMap::from([(id, signal(id, 0.73))]),
            HashSet::new(),
            10,
            2,
        );
        assert_eq!(ranked.len(), 1);
        assert!((ranked[0]["score"].as_f64().unwrap() - 0.0146).abs() < 1e-9);
    }

    #[test]
    fn affinity_supplements_at_most_two_non_base_entries() {
        let ids: Vec<Ulid> = [
            "01ARZ3NDEKTSV4RRFFQ69G5FAV",
            "01ARZ3NDEKTSV4RRFFQ69G5FAW",
            "01ARZ3NDEKTSV4RRFFQ69G5FAX",
            "01ARZ3NDEKTSV4RRFFQ69G5FAY",
        ]
        .into_iter()
        .map(|s| s.parse().unwrap())
        .collect();
        let supplemental = ids[1..].iter().map(|id| candidate(*id, 0.0)).collect();
        let affinities = ids[1..]
            .iter()
            .enumerate()
            .map(|(rank, id)| affinity(*id, rank as u32 + 1, 0.01 - rank as f64 * 0.001))
            .collect();
        let signals = ids.iter().map(|id| (*id, signal(*id, 1.0))).collect();
        let ranked = rank_candidates(
            vec![candidate(ids[0], 0.02)],
            supplemental,
            affinities,
            signals,
            HashSet::new(),
            10,
            2,
        );
        assert_eq!(ranked.len(), 3);
        assert!(
            !ranked
                .iter()
                .any(|v| v["entry"]["id"] == ids[3].to_string())
        );
    }

    #[test]
    fn multiple_affinity_examples_for_one_entry_do_not_stack() {
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let ranked = rank_candidates(
            vec![candidate(id, 0.02)],
            vec![],
            vec![affinity(id, 1, 0.004), affinity(id, 2, 0.009)],
            HashMap::from([(id, signal(id, 1.0))]),
            HashSet::new(),
            10,
            2,
        );
        assert!((ranked[0]["affinity_score"].as_f64().unwrap() - 0.009).abs() < 1e-9);
        assert!((ranked[0]["score"].as_f64().unwrap() - 0.029).abs() < 1e-9);
    }

    #[test]
    fn transient_penalty_multiplies_after_memory_factor() {
        let id: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let ranked = rank_candidates(
            vec![candidate(id, 0.02)],
            vec![],
            vec![],
            HashMap::from([(id, signal(id, 1.10))]),
            HashSet::from([id]),
            10,
            2,
        );
        assert!((ranked[0]["score"].as_f64().unwrap() - 0.011).abs() < 1e-9);
    }

    #[test]
    fn final_sort_is_score_then_base_then_entry_id() {
        let a: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAV".parse().unwrap();
        let b: Ulid = "01ARZ3NDEKTSV4RRFFQ69G5FAW".parse().unwrap();
        let ranked = rank_candidates(
            vec![candidate(b, 0.02), candidate(a, 0.02)],
            vec![],
            vec![],
            HashMap::from([(a, signal(a, 1.0)), (b, signal(b, 1.0))]),
            HashSet::new(),
            10,
            2,
        );
        assert_eq!(ranked[0]["entry"]["id"], a.to_string());
        assert_eq!(ranked[1]["entry"]["id"], b.to_string());
    }
}
