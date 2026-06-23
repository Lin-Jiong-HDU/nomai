//! Object cache benchmark: measure where time goes in the hot path so we
//! can decide whether an in-memory `Entry`/`Chunk` cache is worth the
//! invalidation complexity.
//!
//! Each scenario runs N iterations after a warmup; we report min / p50 /
//! p99 to suppress noise from outliers (GC, scheduler).
//!
//! Run: `cargo run --example bench_object_cache --release`

use std::sync::Arc;
use std::time::{Duration, Instant};

use nomai_core::{
    CreateEntry, CreateLink, Direction, EntryListQuery, EntryService, LinkService, NeighborsQuery,
};
use ulid::Ulid;

fn stats(samples: &mut [Duration]) -> (Duration, Duration, Duration, Duration) {
    samples.sort();
    let n = samples.len();
    let min = samples[0];
    let p50 = samples[n / 2];
    let p99 = samples[(n * 99) / 100];
    let mean: Duration = samples.iter().sum::<Duration>() / n as u32;
    (min, p50, p99, mean)
}

fn bench<F: FnMut()>(name: &str, iters: usize, warmup: usize, mut f: F) -> Duration {
    for _ in 0..warmup {
        f();
    }
    let mut samples: Vec<Duration> = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        f();
        samples.push(t0.elapsed());
    }
    let (min, p50, p99, mean) = stats(&mut samples);
    println!(
        "  {:<42} min={:>7.2}µs  p50={:>7.2}µs  p99={:>7.2}µs  mean={:>7.2}µs",
        name,
        min.as_secs_f64() * 1e6,
        p50.as_secs_f64() * 1e6,
        p99.as_secs_f64() * 1e6,
        mean.as_secs_f64() * 1e6,
    );
    mean
}

fn main() {
    nomai_core::storage::init_sqlite_extensions();
    let conn = Arc::new(std::sync::Mutex::new(
        rusqlite::Connection::open_in_memory().unwrap(),
    ));
    let entries = Arc::new(EntryService::new(conn.clone()).unwrap());
    let links = Arc::new(LinkService::new(conn.clone()).unwrap());

    // ----- Seed: 1000 entries + hub-and-spoke + chain -----
    eprintln!("Seeding 1000 entries + 1019 links...");
    let mut ids: Vec<Ulid> = Vec::with_capacity(1000);
    for i in 0..1000 {
        let e = entries
            .create(CreateEntry {
                title: format!("Entry {i}"),
                body: format!(
                    "Body of entry {i} with some markdown content and references to topic {i}."
                ),
                tags: Some(vec![format!("tag-{}", i % 10), format!("cat-{}", i % 5)]),
                attrs: Some(serde_json::json!({"index": i, "bucket": i / 100})),
                source: None,
            })
            .unwrap();
        ids.push(e.id);
    }

    // Hub: ids[0] references ids[1..20]
    for i in 1..20 {
        links
            .create(CreateLink {
                source_id: ids[0],
                target_id: ids[i],
                relation: "references".into(),
                attrs: None,
            })
            .unwrap();
    }
    // Chain: each entry references the previous one
    for i in 1..1000 {
        links
            .create(CreateLink {
                source_id: ids[i],
                target_id: ids[i - 1],
                relation: "references".into(),
                attrs: None,
            })
            .unwrap();
    }

    // Seed embeddings (deterministic vectors) so semantic_search runs end-to-end.
    entries.ensure_vec_embeddings(8).unwrap();
    for (i, id) in ids.iter().enumerate() {
        let v = [
            ((i as f32) / 1000.0),
            ((i as f32) % 7.0) / 7.0,
            ((i as f32) % 13.0) / 13.0,
            ((i as f32) % 17.0) / 17.0,
            ((i as f32) % 19.0) / 19.0,
            ((i as f32) % 23.0) / 23.0,
            ((i as f32) % 29.0) / 29.0,
            ((i as f32) % 31.0) / 31.0,
        ];
        entries.write_embedding(*id, &v).unwrap();
    }

    let hub = ids[0];
    let _some_id = ids[42];

    println!("\n=== Single-op micro-benchmarks (1000 iters, 100 warmup) ===\n");

    let t_get_unique = bench("entry.get (cold, 50 unique ids)", 1000, 100, || {
        for i in 0..50 {
            let _ = entries.get(ids[i * 19 % 1000]);
        }
    }) / 50;

    let t_get_repeat = bench("entry.get (hot, same id 1000x)", 1000, 100, || {
        let _ = entries.get(hub);
    });

    let t_list = bench("entry.list page (50 items)", 1000, 100, || {
        let _ = entries.list(EntryListQuery {
            tag: None,
            limit: 50,
            offset: 0,
            ..Default::default()
        });
    });

    let t_neighbors = bench("link.neighbors (hub, ~20 neighbors)", 1000, 100, || {
        let _ = links.neighbors(NeighborsQuery {
            id: hub,
            relation: None,
            direction: Direction::Both,
            limit: 50,
        });
    });

    let t_fts = bench("fulltext_search (top 10)", 1000, 100, || {
        let _ = entries.fulltext_search("entry topic", 10);
    });

    let t_semantic = bench("semantic_search (top 10)", 1000, 100, || {
        let _ = entries.semantic_search(&[0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5, 0.5], 10);
    });

    println!("\n=== Per-operation mean cost ===\n");
    println!(
        "  entry.get (unique)        {:>7.2}µs",
        t_get_unique.as_secs_f64() * 1e6
    );
    println!(
        "  entry.get (repeat hub)    {:>7.2}µs",
        t_get_repeat.as_secs_f64() * 1e6
    );
    println!(
        "  entry.list (50 items)     {:>7.2}µs",
        t_list.as_secs_f64() * 1e6
    );
    println!(
        "  link.neighbors            {:>7.2}µs",
        t_neighbors.as_secs_f64() * 1e6
    );
    println!(
        "  fulltext_search           {:>7.2}µs",
        t_fts.as_secs_f64() * 1e6
    );
    println!(
        "  semantic_search           {:>7.2}µs",
        t_semantic.as_secs_f64() * 1e6
    );

    println!("\n=== GraphRAG pattern breakdown ===\n");
    println!("  Pattern: search.semantic(top-3) + 3 × link.neighbors(limit=5)");
    println!("  Note: link.neighbors returns full Entry objects via JOIN,");
    println!("        so GraphRAG does NOT issue separate entry.get calls.\n");
    let graph_rag = t_semantic + t_neighbors * 3;
    println!(
        "  Estimated total:          {:>7.2}µs",
        graph_rag.as_secs_f64() * 1e6
    );
    println!(
        "  - search.semantic share:  {:>6.1}%",
        100.0 * t_semantic.as_secs_f64() / graph_rag.as_secs_f64()
    );
    println!(
        "  - 3 × neighbors share:    {:>6.1}%",
        100.0 * 3.0 * t_neighbors.as_secs_f64() / graph_rag.as_secs_f64()
    );

    println!("\n=== Object cache potential benefit ===\n");
    let cache_hit_estimate = Duration::from_nanos(500); // ~Arc clone + HashMap lookup
    let unique_us = t_get_unique.as_secs_f64() * 1e6;
    let cached_us = cache_hit_estimate.as_secs_f64() * 1e6;
    println!("  entry.get SQLite path:    {:>7.2}µs", unique_us);
    println!(
        "  entry.get cache hit est:  {:>7.2}µs  (Arc clone + HashMap lookup)",
        cached_us
    );
    println!(
        "  Speedup if cache hit:     {:>7.1}x",
        unique_us / cached_us
    );
    println!();
    println!("  But: GraphRAG doesn't call entry.get separately (neighbors JOIN).");
    println!("       entry.list / search already deserialize their own result rows.");
    println!("       The only real beneficiary is the rare 'get same id N times' pattern.");
    println!();
    println!(
        "  Verdict: weigh the {:>6.1}x hit-path speedup against invalidation cost.",
        unique_us / cached_us
    );
}
