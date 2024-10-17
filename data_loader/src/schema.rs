diesel::table! {
    users_product_ai {
        id -> BigInt,
        id_user -> Nullable<BigInt>,
        mac_address -> Nullable<Text>,
        id_product -> BigInt,
        rank -> Integer,
    }
}
