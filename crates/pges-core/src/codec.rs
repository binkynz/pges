//! Codecs for event data. `#[derive(Event)]` calls these; hand-written [`crate::event::Event`] impls can too.

/// JSON through `serde_json`. Enabled by the `json` feature (on by default).
#[cfg(feature = "json")]
pub mod json {
    use serde::{Serialize, de::DeserializeOwned};

    use crate::event;

    /// Turns the JSON of one schema version of an event into the JSON of the next version.
    ///
    /// An error makes decoding fail with [`event::CodecError::Decode`], naming the stored schema version.
    pub type Upcaster = fn(serde_json::Value) -> Result<serde_json::Value, event::BoxError>;

    /// Appends `value` as JSON to `out`.
    pub fn encode<T: Serialize>(
        type_name: &'static str,
        value: &T,
        out: &mut Vec<u8>,
    ) -> Result<(), event::CodecError> {
        serde_json::to_writer(out, value).map_err(|source| event::CodecError::Encode {
            type_name,
            source: source.into(),
        })
    }

    /// Reads `T` from JSON.
    pub fn decode<T: DeserializeOwned>(
        type_name: &str,
        schema_version: event::SchemaVersion,
        data: &[u8],
    ) -> Result<T, event::CodecError> {
        serde_json::from_slice(data)
            .map_err(|source| decode_error(type_name, schema_version, source))
    }

    /// Reads JSON written at an older `schema_version`, applies `upcasters` in order, and reads `T` from the
    /// result.
    ///
    /// `upcasters[0]` takes `schema_version` to `schema_version + 1`, and so on; the last one must produce
    /// the shape of `T`. With no upcasters it behaves like [`decode`]. Fails with
    /// [`event::CodecError::Decode`] when the data is not JSON, an upcaster fails, or the upcast value is not
    /// a valid `T`.
    pub fn decode_upcast<T: DeserializeOwned>(
        type_name: &str,
        schema_version: event::SchemaVersion,
        data: &[u8],
        upcasters: &[Upcaster],
    ) -> Result<T, event::CodecError> {
        let mut value: serde_json::Value = decode(type_name, schema_version, data)?;

        for upcaster in upcasters {
            value = upcaster(value).map_err(|source| event::CodecError::Decode {
                type_name: type_name.to_owned(),
                schema_version,
                source,
            })?;
        }

        serde_json::from_value(value)
            .map_err(|source| decode_error(type_name, schema_version, source))
    }

    fn decode_error(
        type_name: &str,
        schema_version: event::SchemaVersion,
        source: serde_json::Error,
    ) -> event::CodecError {
        event::CodecError::Decode {
            type_name: type_name.to_owned(),
            schema_version,
            source: source.into(),
        }
    }

    #[cfg(test)]
    mod tests {
        use serde::Deserialize;

        use super::*;

        fn v(n: u16) -> event::SchemaVersion {
            event::SchemaVersion::new(n).unwrap()
        }

        #[derive(Debug, PartialEq, Deserialize)]
        struct Note {
            text: String,
            pinned: bool,
        }

        fn rename_body(mut value: serde_json::Value) -> Result<serde_json::Value, event::BoxError> {
            let object = value.as_object_mut().ok_or("not an object")?;
            let body = object.remove("body").ok_or("no body")?;
            object.insert("text".into(), body);

            Ok(value)
        }

        fn add_pinned(mut value: serde_json::Value) -> Result<serde_json::Value, event::BoxError> {
            let object = value.as_object_mut().ok_or("not an object")?;
            object.insert("pinned".into(), false.into());

            Ok(value)
        }

        #[test]
        fn upcasters_run_in_order() {
            let note: Note = decode_upcast(
                "note",
                v(1),
                br#"{"body":"hi"}"#,
                &[rename_body, add_pinned],
            )
            .unwrap();

            assert_eq!(
                note,
                Note {
                    text: "hi".into(),
                    pinned: false
                }
            );
        }

        #[test]
        fn no_upcasters_is_a_plain_decode() {
            let note: Note =
                decode_upcast("note", v(3), br#"{"text":"hi","pinned":true}"#, &[]).unwrap();

            assert!(note.pinned);
        }

        #[test]
        fn upcaster_errors_name_the_stored_version() {
            let error = decode_upcast::<Note>(
                "note",
                v(1),
                br#"{"text":"hi"}"#,
                &[rename_body, add_pinned],
            )
            .unwrap_err();

            assert!(
                matches!(
                    &error,
                    event::CodecError::Decode { type_name, schema_version, .. }
                        if type_name == "note" && schema_version.get() == 1
                ),
                "{error}"
            );
        }

        #[test]
        fn an_upcast_value_of_the_wrong_shape_is_an_error() {
            let error = decode_upcast::<Note>("note", v(2), br#"{"text":"hi"}"#, &[]).unwrap_err();

            assert!(
                matches!(
                    error,
                    event::CodecError::Decode { schema_version, .. } if schema_version.get() == 2
                ),
                "{error}"
            );
        }
    }
}
