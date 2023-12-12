impl ::risingwave_common::row::Row for Data {
    fn datum_at(
        &self,
        index: ::std::primitive::usize,
    ) -> ::risingwave_common::types::DatumRef<'_> {
        match index {
            0usize => {
                <i16 as ::risingwave_common::types::ToDatumRef>::to_datum_ref(&self.v1)
            }
            1usize => {
                <std::primitive::i32 as ::risingwave_common::types::ToDatumRef>::to_datum_ref(
                    &self.v2,
                )
            }
            2usize => {
                <bool as ::risingwave_common::types::ToDatumRef>::to_datum_ref(&self.v3)
            }
            3usize => {
                <Serial as ::risingwave_common::types::ToDatumRef>::to_datum_ref(
                    &self.v4,
                )
            }
            _ => panic!("index out of bounds"),
        }
    }
    fn datum_at_unchecked(
        &self,
        index: ::std::primitive::usize,
    ) -> ::risingwave_common::types::DatumRef<'_> {
        match index {
            0usize => {
                <i16 as ::risingwave_common::types::ToDatumRef>::to_datum_ref(&self.v1)
            }
            1usize => {
                <std::primitive::i32 as ::risingwave_common::types::ToDatumRef>::to_datum_ref(
                    &self.v2,
                )
            }
            2usize => {
                <bool as ::risingwave_common::types::ToDatumRef>::to_datum_ref(&self.v3)
            }
            3usize => {
                <Serial as ::risingwave_common::types::ToDatumRef>::to_datum_ref(
                    &self.v4,
                )
            }
            _ => std::hint::unreachable_unchecked(),
        }
    }
    fn len() -> ::std::primitive::usize {
        4usize
    }
    fn iter(
        &self,
    ) -> impl ::std::iter::Iterator<Item = ::risingwave_common::types::DatumRef<'_>> {
        [
            <i16 as ::risingwave_common::types::ToDatumRef>::to_datum_ref(&self.v1),
            <std::primitive::i32 as ::risingwave_common::types::ToDatumRef>::to_datum_ref(
                &self.v2,
            ),
            <bool as ::risingwave_common::types::ToDatumRef>::to_datum_ref(&self.v3),
            <Serial as ::risingwave_common::types::ToDatumRef>::to_datum_ref(&self.v4),
        ]
    }
}
impl ::risingwave_common::types::Fields for Data {
    fn fields() -> ::std::vec::Vec<
        (&'static ::std::primitive::str, ::risingwave_common::types::DataType),
    > {
        ::std::vec![
            ("v1", < i16 as ::risingwave_common::types::WithDataType >
            ::default_data_type()), ("v2", < std::primitive::i32 as
            ::risingwave_common::types::WithDataType > ::default_data_type()), ("v3", <
            bool as ::risingwave_common::types::WithDataType > ::default_data_type()),
            ("v4", < Serial as ::risingwave_common::types::WithDataType >
            ::default_data_type())
        ]
            .into_iter()
    }
}
