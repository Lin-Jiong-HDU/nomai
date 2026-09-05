use chrono::{DateTime, Utc};
use ulid::Ulid;

use crate::error::CoreError;

pub trait Clock: Send + Sync {
    fn now(&self) -> DateTime<Utc>;
}

#[derive(Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> DateTime<Utc> {
        Utc::now()
    }
}

#[derive(Debug, Clone)]
pub struct MemoryPolicy {
    pub enabled: bool,
    pub half_life_days: f64,
    pub decay_floor: f64,
    pub max_reinforcements: u8,
    pub session_ttl_hours: i64,
    pub affinity_similarity_threshold: f64,
    pub affinity_weight: f64,
    pub affinity_candidate_limit: usize,
    pub entry_recency_weight: f64,
    pub entry_reinforcement_factors: Vec<f64>,
    pub affinity_reinforcement_factors: Vec<f64>,
}

impl Default for MemoryPolicy {
    fn default() -> Self {
        Self {
            enabled: true,
            half_life_days: 30.0,
            decay_floor: 0.10,
            max_reinforcements: 3,
            session_ttl_hours: 24,
            affinity_similarity_threshold: 0.82,
            affinity_weight: 1.0,
            affinity_candidate_limit: 2,
            entry_recency_weight: 0.30,
            entry_reinforcement_factors: vec![1.00, 1.10, 1.17, 1.22],
            affinity_reinforcement_factors: vec![0.00, 0.70, 0.90, 1.00],
        }
    }
}

impl MemoryPolicy {
    pub fn validate(&self) -> Result<(), CoreError> {
        if !self.half_life_days.is_finite() || self.half_life_days <= 0.0 {
            return Err(CoreError::Validation(
                "memory half_life_days must be finite and > 0".into(),
            ));
        }
        if !self.decay_floor.is_finite() || self.decay_floor <= 0.0 || self.decay_floor > 1.0 {
            return Err(CoreError::Validation(
                "memory decay_floor must be finite and in (0, 1]".into(),
            ));
        }
        if !(1..=3).contains(&self.max_reinforcements) {
            return Err(CoreError::Validation(
                "memory max_reinforcements must be in 1..=3".into(),
            ));
        }
        if self.session_ttl_hours <= 0 {
            return Err(CoreError::Validation(
                "memory session_ttl_hours must be > 0".into(),
            ));
        }
        if !self.affinity_similarity_threshold.is_finite()
            || !(0.0..1.0).contains(&self.affinity_similarity_threshold)
        {
            return Err(CoreError::Validation(
                "memory affinity_similarity_threshold must be finite and in [0, 1)".into(),
            ));
        }
        if !self.affinity_weight.is_finite() || self.affinity_weight < 0.0 {
            return Err(CoreError::Validation(
                "memory affinity_weight must be finite and >= 0".into(),
            ));
        }
        if self.affinity_candidate_limit == 0 {
            return Err(CoreError::Validation(
                "memory affinity_candidate_limit must be > 0".into(),
            ));
        }
        if !self.entry_recency_weight.is_finite()
            || !(0.0..=1.0).contains(&self.entry_recency_weight)
        {
            return Err(CoreError::Validation(
                "memory entry_recency_weight must be finite and in [0, 1]".into(),
            ));
        }

        let expected = usize::from(self.max_reinforcements) + 1;
        if self.entry_reinforcement_factors.len() != expected
            || self.affinity_reinforcement_factors.len() != expected
        {
            return Err(CoreError::Validation(format!(
                "memory reinforcement factor arrays must contain {expected} values"
            )));
        }
        let valid_factors = |factors: &[f64]| {
            factors
                .iter()
                .all(|factor| factor.is_finite() && *factor >= 0.0)
                && factors.windows(2).all(|pair| pair[0] <= pair[1])
        };
        if !valid_factors(&self.entry_reinforcement_factors)
            || !valid_factors(&self.affinity_reinforcement_factors)
        {
            return Err(CoreError::Validation(
                "memory reinforcement factors must be finite, non-negative, and non-decreasing"
                    .into(),
            ));
        }
        Ok(())
    }

    pub fn decay(&self, since: DateTime<Utc>, now: DateTime<Utc>) -> f64 {
        let age_days = (now - since).num_seconds() as f64 / 86_400.0;
        2.0_f64
            .powf(-age_days / self.half_life_days)
            .clamp(self.decay_floor, 1.0)
    }

    pub fn entry_reinforcement_factor(&self, reinforcement_count: u8) -> f64 {
        let index = usize::from(reinforcement_count.min(self.max_reinforcements));
        self.entry_reinforcement_factors[index]
    }

    pub fn affinity_reinforcement_factor(&self, reinforcement_count: u8) -> f64 {
        let index = usize::from(reinforcement_count.min(self.max_reinforcements));
        self.affinity_reinforcement_factors[index]
    }

    pub fn entry_memory_factor(
        &self,
        reinforcement_count: u8,
        last_reinforced_at: DateTime<Utc>,
        now: DateTime<Utc>,
    ) -> f64 {
        ((1.0 - self.entry_recency_weight)
            + self.entry_recency_weight * self.decay(last_reinforced_at, now))
            * self.entry_reinforcement_factor(reinforcement_count)
    }

    pub fn normalized_similarity(&self, cosine_similarity: f64) -> f64 {
        ((cosine_similarity - self.affinity_similarity_threshold)
            / (1.0 - self.affinity_similarity_threshold))
            .clamp(0.0, 1.0)
    }
}

#[derive(Debug, Clone)]
pub struct FeedbackTarget {
    pub entry_id: Ulid,
    pub block_id: Option<Ulid>,
    pub chunk_id: Option<Ulid>,
}

#[derive(Debug, Clone)]
pub struct SearchResultTarget {
    pub entry_id: Ulid,
    pub matched_block_id: Option<Ulid>,
    pub matched_chunk_id: Option<Ulid>,
    pub result_rank: u32,
}

#[derive(Debug, Clone)]
pub struct CreateSearchSession {
    pub raw_query_text: String,
    pub effective_query_text: String,
    pub query_embedding: Vec<f32>,
    pub embedding_model: String,
    pub results: Vec<SearchResultTarget>,
}

#[derive(Debug, Clone)]
pub struct EntryMemorySignal {
    pub entry_id: Ulid,
    pub reinforcement_count: u8,
    pub last_reinforced_at: DateTime<Utc>,
    pub memory_factor: f64,
}

#[derive(Debug, Clone)]
pub struct AffinityHit {
    pub entry_id: Ulid,
    pub block_id: Option<Ulid>,
    pub chunk_id: Option<Ulid>,
    pub similarity: f64,
    pub confidence: f64,
    pub affinity_rank: u32,
    pub affinity_score: f64,
}

/// One retained learned-query example that needs a provider vector.
///
/// Multiple ordinary affinity rows can collapse to this survivor when an
/// embedding-model change removes the model name from their identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinityEmbeddingInput {
    pub affinity_id: Ulid,
    pub effective_query_text: String,
}

/// Read-only provider work plus the exact ordinary-affinity snapshot it was
/// derived from. Replacement rejects the plan if that state changes while the
/// provider is running.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinityEmbeddingPlan {
    pub inputs: Vec<AffinityEmbeddingInput>,
    pub fingerprint: String,
}

#[derive(Debug, Clone)]
pub struct AppliedFeedback {
    pub entry_id: Ulid,
    pub reinforcement_count: u8,
    pub affinity_count: u8,
    pub last_reinforced_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct FeedbackResult {
    pub applied: Vec<AppliedFeedback>,
    pub already_applied: Vec<Ulid>,
}

#[cfg(test)]
mod tests {
    use super::MemoryPolicy;
    use crate::CoreError;

    fn assert_policy_rejects(policy: MemoryPolicy, field: &str) {
        assert!(
            matches!(policy.validate(), Err(CoreError::Validation(message)) if message.contains(field)),
            "policy should reject unsafe {field}"
        );
    }

    fn policy_with_cap(max_reinforcements: u8) -> MemoryPolicy {
        let mut policy = MemoryPolicy {
            max_reinforcements,
            ..MemoryPolicy::default()
        };
        match max_reinforcements {
            1 => {
                policy.entry_reinforcement_factors = vec![1.0, 1.1];
                policy.affinity_reinforcement_factors = vec![0.0, 0.7];
            }
            2 => {
                policy.entry_reinforcement_factors = vec![1.0, 1.1, 1.17];
                policy.affinity_reinforcement_factors = vec![0.0, 0.7, 0.9];
            }
            _ => {}
        }
        policy
    }

    #[test]
    fn memory_policy_accepts_supported_non_default_caps() {
        assert!(policy_with_cap(1).validate().is_ok());
        assert!(policy_with_cap(2).validate().is_ok());
    }

    #[test]
    fn memory_policy_rejects_caps_outside_database_range() {
        for max_reinforcements in [0, 4] {
            assert!(matches!(
                policy_with_cap(max_reinforcements).validate(),
                Err(CoreError::Validation(message))
                    if message.contains("max_reinforcements")
            ));
        }
    }

    #[test]
    fn memory_policy_rejects_factor_arrays_of_the_wrong_length() {
        let mut policy = policy_with_cap(2);
        policy.affinity_reinforcement_factors.pop();
        assert!(matches!(
            policy.validate(),
            Err(CoreError::Validation(message)) if message.contains("3 values")
        ));
    }

    #[test]
    fn memory_policy_rejects_non_monotonic_factor_arrays() {
        let mut entry = policy_with_cap(2);
        entry.entry_reinforcement_factors = vec![1.0, 1.2, 1.1];
        assert!(matches!(
            entry.validate(),
            Err(CoreError::Validation(message)) if message.contains("non-decreasing")
        ));

        let mut affinity = policy_with_cap(2);
        affinity.affinity_reinforcement_factors = vec![0.0, 0.9, 0.8];
        assert!(matches!(
            affinity.validate(),
            Err(CoreError::Validation(message)) if message.contains("non-decreasing")
        ));
    }

    #[test]
    fn memory_policy_rejects_non_finite_or_unsafe_scalar_ranges() {
        for half_life_days in [0.0, -1.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert_policy_rejects(
                MemoryPolicy {
                    half_life_days,
                    ..MemoryPolicy::default()
                },
                "half_life_days",
            );
        }
        for decay_floor in [0.0, -0.1, 1.1, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert_policy_rejects(
                MemoryPolicy {
                    decay_floor,
                    ..MemoryPolicy::default()
                },
                "decay_floor",
            );
        }
        for session_ttl_hours in [0, -1] {
            assert_policy_rejects(
                MemoryPolicy {
                    session_ttl_hours,
                    ..MemoryPolicy::default()
                },
                "session_ttl_hours",
            );
        }
        for affinity_similarity_threshold in [-0.1, 1.0, f64::INFINITY, f64::NEG_INFINITY, f64::NAN]
        {
            assert_policy_rejects(
                MemoryPolicy {
                    affinity_similarity_threshold,
                    ..MemoryPolicy::default()
                },
                "affinity_similarity_threshold",
            );
        }
        for affinity_weight in [-0.1, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert_policy_rejects(
                MemoryPolicy {
                    affinity_weight,
                    ..MemoryPolicy::default()
                },
                "affinity_weight",
            );
        }
        assert_policy_rejects(
            MemoryPolicy {
                affinity_candidate_limit: 0,
                ..MemoryPolicy::default()
            },
            "affinity_candidate_limit",
        );
        for entry_recency_weight in [-0.1, 1.1, f64::INFINITY, f64::NEG_INFINITY, f64::NAN] {
            assert_policy_rejects(
                MemoryPolicy {
                    entry_recency_weight,
                    ..MemoryPolicy::default()
                },
                "entry_recency_weight",
            );
        }
    }

    #[test]
    fn memory_policy_rejects_non_finite_negative_or_decreasing_factors() {
        for (index, invalid) in [
            (0, -0.1),
            (3, f64::INFINITY),
            (0, f64::NEG_INFINITY),
            (0, f64::NAN),
        ] {
            let mut entry = MemoryPolicy::default();
            entry.entry_reinforcement_factors[index] = invalid;
            assert_policy_rejects(entry, "reinforcement factors");

            let mut affinity = MemoryPolicy::default();
            affinity.affinity_reinforcement_factors[index] = invalid;
            assert_policy_rejects(affinity, "reinforcement factors");
        }
    }

    #[test]
    fn decay_has_thirty_day_half_life_and_floor() {
        let policy = MemoryPolicy::default();
        let start = "2026-01-01T00:00:00Z".parse().unwrap();
        assert!((policy.decay(start, start + chrono::Duration::days(30)) - 0.5).abs() < 1e-9);
        assert!((policy.decay(start, start + chrono::Duration::days(60)) - 0.25).abs() < 1e-9);
        assert_eq!(
            policy.decay(start, start + chrono::Duration::days(365)),
            0.10
        );
    }

    #[test]
    fn decay_for_a_future_timestamp_is_clamped_to_one() {
        let policy = MemoryPolicy::default();
        let now = "2026-01-01T00:00:00Z".parse().unwrap();
        let future = now + chrono::Duration::days(30);

        assert_eq!(policy.decay(future, now), 1.0);
    }

    #[test]
    fn reinforcement_factors_are_bounded() {
        let policy = MemoryPolicy::default();
        assert_eq!(policy.entry_reinforcement_factor(0), 1.00);
        assert_eq!(policy.entry_reinforcement_factor(3), 1.22);
        assert_eq!(policy.entry_reinforcement_factor(99), 1.22);
        assert_eq!(policy.affinity_reinforcement_factor(1), 0.70);
        assert_eq!(policy.affinity_reinforcement_factor(99), 1.00);
    }

    #[test]
    fn entry_factor_blends_decay_instead_of_erasing_old_results() {
        let policy = MemoryPolicy::default();
        let start = "2026-01-01T00:00:00Z".parse().unwrap();
        let old = policy.entry_memory_factor(0, start, start + chrono::Duration::days(365));
        assert!((old - 0.73).abs() < 1e-9);
    }
}
