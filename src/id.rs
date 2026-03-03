use rand::Rng;

/// Generate a hash-based task ID like `lt-a1b2`
pub fn generate() -> String {
    let mut rng = rand::rng();
    let hex: String = (0..4).map(|_| format!("{:x}", rng.random_range(0..16u8))).collect();
    format!("lt-{hex}")
}
