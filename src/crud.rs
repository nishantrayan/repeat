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
        if self.card_exists(card).await? {
            return Ok(());
        }
        let now = chrono::Utc::now().to_rfc3339();

        sqlx::query(
            r#"
        INSERT INTO cards (
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
            if self.card_exists(card).await? {
                continue;
            }

            sqlx::query(
                r#"
            INSERT INTO cards (
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

    pub async fn due_today(
        &self,
        card_hashes: HashMap<String, Card>,
        card_limit: Option<usize>,
    ) -> Result<Vec<Card>> {
        let today = chrono::Local::now().date_naive();

        let sql = "SELECT card_hash 
           FROM cards
           WHERE due_date <= ? OR due_date IS NULL;";
        let mut rows = sqlx::query(sql).bind(today).fetch(&self.pool);
        let mut cards = Vec::new();
        while let Some(row) = rows.try_next().await? {
            let card_hash: String = row.get("card_hash");
            if !card_hashes.contains_key(&card_hash) {
                continue;
            }

            if let Some(card) = card_hashes.get(&card_hash) {
                cards.push(card.clone());
            }

            if let Some(card_limit) = card_limit {
                if cards.len() >= card_limit {
                    break;
                }
            }
        }

        Ok(cards)
    }
}

async fn probe_schema_exists(pool: &SqlitePool) -> Result<bool, sqlx::Error> {
    let sql = "select count(*) from sqlite_master where type='table' AND name=?;";

    let count: (i64,) = sqlx::query_as(sql).bind("cards").fetch_one(pool).await?;
    Ok(count.0 > 0)
}
