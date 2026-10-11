//! Server-rendered starting state; browser updates use the same exact strings.
use crate::runner::Published;
use std::fmt::Write;

/// Initial HTML is useful before JavaScript loads; all metadata text is escaped.
pub fn page(report: &Published) -> String {
    let mut books = String::new();
    for market in &report.view.markets {
        let _ = write!(
            books,
            "<section class=panel data-market-id=\"{}\"><h2>{}</h2><p>{} · sequence {}</p><div class=ladders>",
            escape(&market.id),
            escape(&market.name),
            market.status,
            market.sequence.as_deref().unwrap_or("—")
        );
        for (name, levels) in [("Bids", &market.bids), ("Asks", &market.asks)] {
            let _ = write!(
                books,
                "<div><h3>{name}</h3><table><thead><tr><th>Price</th><th>Quantity</th></tr></thead><tbody>"
            );
            for level in levels {
                let _ = write!(
                    books,
                    "<tr><td>{}</td><td>{}</td></tr>",
                    level.price, level.quantity
                );
            }
            books.push_str("</tbody></table></div>");
        }
        books.push_str("</div></section>");
    }
    include_str!("assets/book.html").replace("{{BOOKS}}", &books)
}
fn escape(value: &str) -> String {
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}
