use std::collections::{BTreeMap, HashMap};

use std::path::PathBuf;

use crate::card::Card;

#[derive(Debug, Default)]
pub struct CardStats {
    pub total_cards_in_db: i64,
    pub num_cards: i64,
    pub card_lifecycles: HashMap<CardLifeCycle, i64>,
    pub due_cards: i64,
    pub overdue_cards: i64,
    pub upcoming_week: BTreeMap<String, usize>,
    pub upcoming_month: i64,
    pub file_paths: HashMap<PathBuf, usize>,
    pub difficulty_histogram: Histogram<20>,
}

#[derive(Debug, Clone)]
pub struct Histogram<const N: usize> {
    bins: [u32; N],
    count: u64,
    sum: f64,
}

impl<const N: usize> Default for Histogram<N> {
    #[inline]
    fn default() -> Self {
        Self {
            bins: [0; N],
            count: 0,
            sum: 0.0,
        }
    }
}
impl<const N: usize> Histogram<N> {
    pub fn update(&mut self, value: f64) {
        let v = value.clamp(0.0, 1.0);
        let mut idx = (v * N as f64) as usize;
        idx = idx.min(N - 1);
        self.bins[idx] += 1;
        self.count += 1;
        self.sum += value;
    }
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub enum CardLifeCycle {
    New,
    Young,
    Mature,
}
const MATURE_INTERVAL: f64 = 21.0;

impl CardStats {
    pub fn update(
        &mut self,
        card: &Card,
        review_count: i64,
        due_date: Option<chrono::DateTime<chrono::Utc>>,
        interval: f64,
        difficulty: f64,
    ) {
        let now = chrono::Utc::now();
        let week_horizon = now + chrono::Duration::days(7);
        let month_horizon = now + chrono::Duration::days(30);
        *self.file_paths.entry(card.file_path.clone()).or_insert(0) += 1;

        let lifecycle = if review_count == 0 {
            CardLifeCycle::New
        } else if interval > MATURE_INTERVAL {
            CardLifeCycle::Mature
        } else {
            CardLifeCycle::Young
        };

        *self.card_lifecycles.entry(lifecycle).or_insert(0) += 1;

        match due_date {
            None => {
                self.due_cards += 1;
            }
            Some(due_date) => {
                if due_date <= now {
                    self.due_cards += 1;
                    if due_date < now {
                        self.overdue_cards += 1;
                    }
                } else {
                    if due_date <= week_horizon {
                        let day = due_date.format("%Y-%m-%d").to_string();
                        *self.upcoming_week.entry(day).or_insert(0) += 1;
                    }

                    if due_date <= month_horizon {
                        self.upcoming_month += 1;
                    }
                }
            }
        }
        self.difficulty_histogram.update(difficulty);
    }
}
