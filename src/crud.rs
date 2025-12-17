use anyhow::Result;
use directories::ProjectDirs;
use futures::TryStreamExt;
use sqlx::Row;
use sqlx::SqlitePool;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};

use std::collections::HashMap;
use std::path::PathBuf;
use std::str::FromStr;

use anyhow::anyhow;

use crate::card::Card;
use crate::fsrs::Performance;
use crate::fsrs::ReviewStatus;
use crate::fsrs::ReviewedPerformance;
use crate::fsrs::update_performance;

#[derive(Debug, Default)]
pub struct CardStats {
    pub total_cards: i64,
    pub new_cards: i64,
    pub reviewed_cards: i64,
    pub due_cards: i64,
    pub overdue_cards: i64,
    pub upcoming_week: Vec<UpcomingCount>,
    pub upcoming_month: i64,
}

#[derive(Debug, Clone)]
pub struct UpcomingCount {
    pub day: String,
    pub count: i64,
}

pub struct DB {
    pool: SqlitePool,
}

impl DB {
    pub async fn new() -> Result<Self> {
        let proj_dirs = ProjectDirs::from("", "", "repeat")
            .ok_or_else(|| anyhow!("Could not determine project directory"))?;
        let data_dir = proj_dirs.data_dir();
        std::fs::create_dir_all(data_dir)
            .map_err(|e| anyhow!("Failed to create data directory: {}", e))?;

        let db_path: PathBuf = data_dir.join("cards.db");
        let options =
            SqliteConnectOptions::from_str(&db_path.to_string_lossy())?.create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(5)
            .connect_with(options)
            .await?;
        let table_exists = probe_schema_exists(&pool).await;
        if let Ok(false) = table_exists {
            sqlx::query(include_str!("schema.sql"))
                .execute(&pool)
                .await?;
        }

        Ok(Self { pool })
    }

    pub async fn add_card(&self, card: &Card) -> Result<()> {
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            r#"
        INSERT or ignore INTO cards (
            card_hash,
            added_at,
            last_reviewed_at,
            stability,
            difficulty,
            interval_raw,
            interval_days,
            due_date,
            review_count
        )
        VALUES (?, ?, NULL, NULL, NULL, NULL, 0, NULL, 0)
        "#,
        )
        .bind(&card.card_hash)
        .bind(now)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn add_cards_batch(&self, cards: &[Card]) -> Result<()> {
        let mut tx = self.pool.begin().await?;

        let now = chrono::Utc::now().to_rfc3339();

        for card in cards {
            sqlx::query(
                r#"
            INSERT or ignore INTO cards (
                card_hash,
                added_at,
                last_reviewed_at,
                stability,
                difficulty,
                interval_raw,
                interval_days,
                due_date,
                review_count
            )
            VALUES (?, ?, NULL, NULL, NULL, NULL, 0, NULL, 0)
            "#,
            )
            .bind(&card.card_hash)
            .bind(&now)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn card_exists(&self, card: &Card) -> Result<bool> {
        let (count,): (i64,) = sqlx::query_as("SELECT COUNT(1) FROM cards WHERE card_hash = ?")
            .bind(&card.card_hash)
            .fetch_one(&self.pool)
            .await?;
        Ok(count > 0)
    }

    pub async fn update_card_performance(
        &self,
        card: &Card,
        review_status: ReviewStatus,
    ) -> Result<bool> {
        let current_performance = self.get_card_performance(card).await?;
        let now = chrono::Utc::now();
        let new_performance = update_performance(current_performance, review_status, now);
        let card_hash = card.card_hash.clone();

        let result = sqlx::query(
            r#"
            UPDATE cards
            SET
                last_reviewed_at = ?,
                stability = ?,
                difficulty = ?,
                interval_raw = ?,
                interval_days = ?,
                due_date = ?,
                review_count = ?
            WHERE card_hash = ?
            "#,
        )
        .bind(new_performance.last_reviewed_at)
        .bind(new_performance.stability)
        .bind(new_performance.difficulty)
        .bind(new_performance.interval_raw)
        .bind(new_performance.interval_days as i64)
        .bind(new_performance.due_date)
        .bind(new_performance.review_count as i64)
        .bind(card_hash)
        .execute(&self.pool)
        .await?;

        Ok(result.rows_affected() > 0)
    }

    pub async fn get_card_performance(&self, card: &Card) -> Result<Performance> {
        let card_hash = card.card_hash.clone();
        let sql = "SELECT added_at, last_reviewed_at, stability, difficulty, interval_raw, interval_days, due_date, review_count 
           FROM cards
           WHERE card_hash = ?;";

        let row = sqlx::query(sql)
            .bind(card_hash)
            .fetch_one(&self.pool)
            .await?;

        let review_count: i64 = row.get("review_count");
        if review_count == 0 {
            return Ok(Performance::default());
        }
        let reviewed = ReviewedPerformance {
            last_reviewed_at: row.get("last_reviewed_at"),
            stability: row.get("stability"),
            difficulty: row.get("difficulty"),
            interval_raw: row.get("interval_raw"),
            interval_days: row.get::<i64, _>("interval_days") as usize,
            due_date: row.get("due_date"),
            review_count: review_count as usize,
        };

        Ok(Performance::Reviewed(reviewed))
    }

    pub async fn due_today(
        &self,
        card_hashes: HashMap<String, Card>,
        card_limit: Option<usize>,
    ) -> Result<Vec<Card>> {
        let now = chrono::Utc::now().to_rfc3339();

        let sql = "SELECT card_hash 
           FROM cards
           WHERE due_date <= ? OR due_date IS NULL;";
        let mut rows = sqlx::query(sql).bind(now).fetch(&self.pool);
        let mut cards = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let card_hash: String = row.get("card_hash");
            if !card_hashes.contains_key(&card_hash) {
                continue;
            }

            if let Some(card) = card_hashes.get(&card_hash) {
                cards.push(card.clone());
            }

            if let Some(card_limit) = card_limit
                && cards.len() >= card_limit
            {
                break;
            }
        }

        Ok(cards)
    }

    pub async fn collection_stats(&self) -> Result<CardStats> {
        let now_dt = chrono::Utc::now();
        let now = now_dt.to_rfc3339();
        let week_horizon = (now_dt + chrono::Duration::days(7)).to_rfc3339();
        let month_horizon = (now_dt + chrono::Duration::days(30)).to_rfc3339();

        let row = sqlx::query(
            r#"
            SELECT
                COUNT(*) AS total_cards,
                COALESCE(SUM(CASE WHEN review_count = 0 THEN 1 ELSE 0 END), 0) AS new_cards,
                COALESCE(SUM(CASE WHEN review_count > 0 THEN 1 ELSE 0 END), 0) AS reviewed_cards,
                COALESCE(SUM(CASE WHEN due_date IS NULL OR due_date <= ? THEN 1 ELSE 0 END), 0) AS due_cards,
                COALESCE(SUM(CASE WHEN due_date IS NOT NULL AND due_date < ? THEN 1 ELSE 0 END), 0) AS overdue_cards
            FROM cards
            "#,
        )
        .bind(&now)
        .bind(&now)
        .fetch_one(&self.pool)
        .await?;

        let upcoming_month: (i64,) = sqlx::query_as(
            r#"
            SELECT
                COALESCE(COUNT(1), 0) AS upcoming_month
            FROM cards
            WHERE due_date IS NOT NULL
              AND due_date > ?
              AND due_date <= ?
            "#,
        )
        .bind(&now)
        .bind(&month_horizon)
        .fetch_one(&self.pool)
        .await?;

        let mut upcoming_week = Vec::new();
        let mut rows = sqlx::query(
            r#"
            SELECT
                strftime('%Y-%m-%d', due_date) AS due_day,
                COUNT(1) AS count
            FROM cards
            WHERE due_date IS NOT NULL
              AND due_date > ?
              AND due_date <= ?
            GROUP BY due_day
            ORDER BY due_day
            "#,
        )
        .bind(&now)
        .bind(&week_horizon)
        .fetch(&self.pool);

        while let Some(row) = rows.try_next().await? {
            let day: Option<String> = row.try_get("due_day")?;
            let count: i64 = row.get("count");
            if let Some(day) = day {
                upcoming_week.push(UpcomingCount { day, count });
            }
        }

        Ok(CardStats {
            total_cards: row.get("total_cards"),
            new_cards: row.get("new_cards"),
            reviewed_cards: row.get("reviewed_cards"),
            due_cards: row.get("due_cards"),
            overdue_cards: row.get("overdue_cards"),
            upcoming_week,
            upcoming_month: upcoming_month.0,
        })
    }
}

async fn probe_schema_exists(pool: &SqlitePool) -> Result<bool, sqlx::Error> {
    let sql = "select count(*) from sqlite_master where type='table' AND name=?;";

    let count: (i64,) = sqlx::query_as(sql).bind("cards").fetch_one(pool).await?;
    Ok(count.0 > 0)
}
