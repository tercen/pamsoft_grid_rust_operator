//! Operator property reading, mirroring the ggrs_plot_operator pattern.
//!
//! `operator.json` is embedded at compile time as the single source of
//! truth for property defaults and metadata (kind, valid enum values).
//! `OperatorPropertyReader` overlays the user-set values from
//! `OperatorSettings` and provides typed getters with validation.
//!
//! Failure mode is fail-loud: invalid user values produce `Err(String)`
//! rather than silently falling back. The operator caller is expected to
//! propagate these as Tercen task failures.

use std::collections::HashMap;
use tercen_rs::client::proto::OperatorSettings;

/// `operator.json` embedded at compile time. The file at the repo root.
const OPERATOR_JSON: &str = include_str!("../operator.json");

#[derive(Debug, Clone, PartialEq)]
pub enum PropertyKind {
    String,
    Double,
    Enumerated,
}

#[derive(Debug, Clone)]
pub struct PropertyDef {
    pub name: String,
    pub kind: PropertyKind,
    /// Default rendered as a string (parsed on demand by the getters).
    pub default_value: String,
    pub valid_values: Option<Vec<String>>,
}

pub struct PropertyRegistry {
    properties: HashMap<String, PropertyDef>,
}

impl PropertyRegistry {
    fn from_operator_json() -> Self {
        let json: serde_json::Value =
            serde_json::from_str(OPERATOR_JSON).expect("operator.json is invalid JSON");
        let arr = json["properties"]
            .as_array()
            .expect("operator.json missing 'properties' array");

        let mut properties = HashMap::new();
        for prop in arr {
            let name = prop["name"]
                .as_str()
                .expect("property missing 'name'")
                .to_string();
            let kind_str = prop["kind"].as_str().expect("property missing 'kind'");
            let kind = match kind_str {
                "StringProperty" => PropertyKind::String,
                "DoubleProperty" => PropertyKind::Double,
                "EnumeratedProperty" => PropertyKind::Enumerated,
                other => panic!("Unknown property kind in operator.json: {}", other),
            };

            // Default value rendered as a string regardless of underlying JSON
            // type — DoubleProperty stores it as a number, the rest as string.
            let default_value = match &prop["defaultValue"] {
                v if v.is_string() => v.as_str().unwrap().to_string(),
                v if v.is_number() => v.to_string(),
                v if v.is_boolean() => v.as_bool().unwrap().to_string(),
                _ => String::new(),
            };

            let valid_values = if kind == PropertyKind::Enumerated {
                prop["values"]
                    .as_array()
                    .map(|arr| arr.iter().filter_map(|v| v.as_str().map(String::from)).collect())
            } else {
                None
            };

            properties.insert(
                name.clone(),
                PropertyDef {
                    name,
                    kind,
                    default_value,
                    valid_values,
                },
            );
        }

        Self { properties }
    }

    fn get_default(&self, name: &str) -> Option<&str> {
        self.properties.get(name).map(|p| p.default_value.as_str())
    }

    fn get(&self, name: &str) -> Option<&PropertyDef> {
        self.properties.get(name)
    }

    fn is_valid_enum_value(&self, name: &str, value: &str) -> bool {
        self.properties
            .get(name)
            .and_then(|p| p.valid_values.as_ref())
            .map(|vs| vs.iter().any(|v| v == value))
            .unwrap_or(false)
    }
}

fn registry() -> &'static PropertyRegistry {
    static INSTANCE: std::sync::OnceLock<PropertyRegistry> = std::sync::OnceLock::new();
    INSTANCE.get_or_init(PropertyRegistry::from_operator_json)
}

/// Reader for typed operator property access. Layers user-set values
/// (from `OperatorSettings`) on top of operator.json defaults.
pub struct OperatorPropertyReader {
    user_values: HashMap<String, String>,
}

impl OperatorPropertyReader {
    pub fn new(settings: Option<&OperatorSettings>) -> Self {
        let user_values = settings
            .and_then(|s| s.operator_ref.as_ref())
            .map(|op| {
                op.property_values
                    .iter()
                    .filter(|p| !p.value.is_empty())
                    .map(|p| (p.name.clone(), p.value.clone()))
                    .collect()
            })
            .unwrap_or_default();
        Self { user_values }
    }

    fn raw(&self, name: &str) -> String {
        if let Some(v) = self.user_values.get(name) {
            return v.clone();
        }
        registry().get_default(name).unwrap_or("").to_string()
    }

    pub fn get_string(&self, name: &str) -> String {
        self.raw(name)
    }

    pub fn get_f64(&self, name: &str) -> Result<f64, String> {
        let v = self.raw(name);
        if v.is_empty() {
            return Err(format!("property '{}' has no value and no default", name));
        }
        v.parse::<f64>()
            .map_err(|_| format!("invalid numeric value '{}' for property '{}'", v, name))
    }

    pub fn get_enum(&self, name: &str) -> Result<String, String> {
        let v = self.raw(name);
        let reg = registry();
        // Default already known-valid; user values get validated.
        if let Some(user_v) = self.user_values.get(name) {
            if !reg.is_valid_enum_value(name, user_v) {
                let valid = reg
                    .get(name)
                    .and_then(|p| p.valid_values.as_ref())
                    .map(|vs| vs.join(", "))
                    .unwrap_or_default();
                return Err(format!(
                    "invalid value '{}' for property '{}'. valid values: [{}]",
                    user_v, name, valid
                ));
            }
        }
        Ok(v)
    }
}

/// Typed container for the pamsoft_grid operator's settings, in algorithm
/// terms (matching the field names of `pamsoft_grid::config::GridParams`
/// and the production R operator's JSON keys).
///
/// `spot_pitch == 0.0` means "auto-detect from image dimensions" — the
/// caller (algorithm wiring) handles this by inspecting the first TIFF
/// and substituting the correct default (17.0 for Evolve3, 21.5 for
/// Evolve2). Same convention as the R operator (aux_functions.R:148-156).
#[derive(Debug, Clone)]
pub struct PamsoftProps {
    pub min_diameter: f64,
    pub max_diameter: f64,
    pub spot_pitch: f64,
    pub spot_size: f64,
    /// Rotation candidates in degrees. Production default `seq(-2, 2, 0.25)`
    /// (17 values). A single value triggers MATLAB's imregister2 path
    /// which the Rust algorithm doesn't implement (and which hallucinates
    /// large rotations in MATLAB anyway — see GRID_PERFORMANCE.md).
    pub rotation: Vec<f64>,
    pub saturation_limit: f64,
    /// `[low, high]` Canny thresholds (fractional). The R operator
    /// declares `EdgeSensitivityLow` but never reads it (hardcodes 0);
    /// we honour it so users can actually tune the lower bound.
    pub edge_sensitivity: [f64; 2],
    pub seg_method: String,
}

/// Read the operator's properties from a `OperatorSettings` proto.
/// Returns informative errors (not panics) when a user-set value is
/// invalid — the caller should surface those as Tercen task failures.
pub fn read_pamsoft_props(settings: Option<&OperatorSettings>) -> Result<PamsoftProps, String> {
    let r = OperatorPropertyReader::new(settings);
    let edge_low = r.get_f64("EdgeSensitivityLow")?;
    let edge_high = r.get_f64("Edge Sensitivity")?;
    Ok(PamsoftProps {
        min_diameter: r.get_f64("Min Diameter")?,
        max_diameter: r.get_f64("Max Diameter")?,
        spot_pitch: r.get_f64("Spot Pitch")?,
        spot_size: r.get_f64("Spot Size")?,
        rotation: parse_rotation(&r.get_string("Rotation"))?,
        saturation_limit: r.get_f64("Saturation Limit")?,
        edge_sensitivity: [edge_low, edge_high],
        seg_method: r.get_enum("Segmentation Method")?,
    })
}

/// Parse the `Rotation` property's MATLAB seq() syntax `min:step:max`,
/// or the special string `"0"` which means "rotation = 0° only" (which
/// the algorithm caller is expected to widen to a 17-element vector,
/// since single-rotation triggers MATLAB's broken imregister2 path —
/// our algorithm also can't handle single-value the way the R operator
/// intends).
///
/// Mirrors `aux_functions.R:112-122`.
fn parse_rotation(s: &str) -> Result<Vec<f64>, String> {
    let s = s.trim();
    if s == "0" {
        return Ok(vec![0.0]);
    }
    let parts: Vec<f64> = s
        .split(':')
        .map(|p| {
            p.trim()
                .parse::<f64>()
                .map_err(|_| format!("invalid rotation token '{}' in '{}'", p, s))
        })
        .collect::<Result<Vec<_>, _>>()?;
    if parts.len() != 3 {
        return Err(format!(
            "rotation must be in 'min:step:max' syntax (e.g. '-2:0.25:2'), got '{}'",
            s
        ));
    }
    let (min, step, max) = (parts[0], parts[1], parts[2]);
    if step <= 0.0 {
        return Err(format!("rotation step must be > 0, got {}", step));
    }
    if max < min {
        return Err(format!("rotation max ({}) < min ({})", max, min));
    }
    let n = ((max - min) / step).round() as usize + 1;
    Ok((0..n).map(|i| min + (i as f64) * step).collect())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_rotation_production_default() {
        let v = parse_rotation("-2:0.25:2").unwrap();
        assert_eq!(v.len(), 17);
        assert!((v[0] - -2.0).abs() < 1e-9);
        assert!((v[16] - 2.0).abs() < 1e-9);
    }

    #[test]
    fn parse_rotation_single_value() {
        assert_eq!(parse_rotation("0").unwrap(), vec![0.0]);
    }

    #[test]
    fn parse_rotation_rejects_zero_step() {
        assert!(parse_rotation("0:0:2").is_err());
    }

    #[test]
    fn parse_rotation_rejects_wrong_arity() {
        assert!(parse_rotation("0:1").is_err());
        assert!(parse_rotation("0:1:2:3").is_err());
    }

    #[test]
    fn defaults_load_from_operator_json() {
        // No user settings → all values come from operator.json defaults.
        let props = read_pamsoft_props(None).unwrap();
        assert!((props.min_diameter - 0.45).abs() < 1e-9);
        assert!((props.max_diameter - 0.85).abs() < 1e-9);
        assert!((props.spot_size - 0.66).abs() < 1e-9);
        assert!((props.saturation_limit - 4095.0).abs() < 1e-9);
        assert_eq!(props.seg_method, "Edge");
        assert_eq!(props.rotation.len(), 17);
    }
}
