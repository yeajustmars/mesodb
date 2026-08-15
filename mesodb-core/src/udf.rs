// mesodb-core/src/udf.rs

use arrow::array::{Array, BooleanBuilder, UInt64Array};
use arrow::datatypes::DataType;
use datafusion::common::ScalarValue;
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::{
    ColumnarValue, ScalarFunctionArgs, ScalarUDFImpl, Signature, Volatility,
};
use roaring::RoaringTreemap;
use std::any::Any;
use std::sync::Arc;

#[derive(Debug)]
pub struct InBitmapUDF {
    signature: Signature,
    treemap: Arc<RoaringTreemap>,
    name: String,
    dirty_entities: Arc<ahash::AHashSet<u64>>,
}

impl InBitmapUDF {
    pub fn new(
        treemap: RoaringTreemap,
        name: String,
        dirty_entities: Arc<ahash::AHashSet<u64>>,
    ) -> Self {
        Self {
            signature: Signature::exact(vec![DataType::UInt64], Volatility::Immutable),
            treemap: Arc::new(treemap),
            name,
            dirty_entities,
        }
    }
}

impl std::hash::Hash for InBitmapUDF {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.name.hash(state);
    }
}

impl PartialEq for InBitmapUDF {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for InBitmapUDF {}

impl ScalarUDFImpl for InBitmapUDF {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn name(&self) -> &str {
        &self.name
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn return_type(&self, _arg_types: &[DataType]) -> DFResult<DataType> {
        Ok(DataType::Boolean)
    }

    fn invoke_with_args(&self, args: ScalarFunctionArgs) -> DFResult<ColumnarValue> {
        match &args.args[0] {
            ColumnarValue::Array(arr) => {
                let e_col = arr.as_any().downcast_ref::<UInt64Array>().unwrap();
                let mut builder = BooleanBuilder::with_capacity(e_col.len());

                for i in 0..e_col.len() {
                    if e_col.is_null(i) {
                        builder.append_null();
                    } else {
                        let val = e_col.value(i);
                        // Let it pass if it's on disk OR if it's dirty in RAM
                        builder.append_value(
                            self.treemap.contains(val) || self.dirty_entities.contains(&val),
                        );
                    }
                }

                Ok(ColumnarValue::Array(Arc::new(builder.finish())))
            }
            ColumnarValue::Scalar(s) => {
                if let ScalarValue::UInt64(Some(val)) = s {
                    let result = self.treemap.contains(*val) || self.dirty_entities.contains(val);
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(Some(result))))
                } else {
                    Ok(ColumnarValue::Scalar(ScalarValue::Boolean(None)))
                }
            }
        }
    }
}
