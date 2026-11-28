//! Subscription names read at run time never panic the validator, which agrees with the documented rule:
//! 1 to 100 bytes, a lowercase ASCII letter first, then `a-z`, `0-9`, `_`, `.` or `-`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic)]

use proptest::prelude::*;

use pges_subscribe::subscription;

proptest! {
    #[test]
    fn subscription_names_follow_the_documented_rule(text in prop_oneof![
        4 => "[a-z_0-9A-Z.\\-]{0,105}",
        1 => any::<String>(),
    ]) {
        let bytes = text.as_bytes();
        let expected = !bytes.is_empty()
            && bytes.len() <= subscription::SubscriptionName::MAX_LEN
            && bytes[0].is_ascii_lowercase()
            && bytes[1..]
                .iter()
                .all(|b| matches!(b, b'a'..=b'z' | b'0'..=b'9' | b'_' | b'.' | b'-'));
        match subscription::SubscriptionName::parse(text.clone()) {
            Ok(name) => {
                prop_assert!(expected, "{text:?}");
                prop_assert_eq!(name.as_str(), text.as_str());
            }
            Err(_) => prop_assert!(!expected, "{text:?}"),
        }
    }
}
