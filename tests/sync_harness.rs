//! Stage 0 sync harness tests.
//!
//! These tests validate CRDT invariants in isolation (no MQTT, no filesystem).

use yrs::updates::decoder::Decode;
use yrs::{Doc, GetString, ReadTxn, StateVector, Text, Transact, Update, WriteTxn};

fn apply_update(doc: &Doc, update: &[u8]) {
    let update = Update::decode_v1(update).expect("update decode failed");
    let mut txn = doc.transact_mut();
    txn.apply_update(update);
}

fn doc_text(doc: &Doc) -> String {
    let txn = doc.transact();
    match txn.get_text("content") {
        Some(text) => text.get_string(&txn),
        None => String::new(),
    }
}

fn base_state(text: &str) -> Vec<u8> {
    let doc = Doc::with_client_id(0);
    {
        let mut txn = doc.transact_mut();
        let root = txn.get_or_insert_text("content");
        root.insert(&mut txn, 0, text);
    }
    let txn = doc.transact();
    txn.encode_state_as_update_v1(&StateVector::default())
}

#[test]
fn crdt_idempotent_apply() {
    let doc_src = Doc::with_client_id(1);
    let update = {
        let mut txn = doc_src.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.insert(&mut txn, 0, "hello");
        txn.encode_update_v1()
    };

    let receiver = Doc::with_client_id(2);
    apply_update(&receiver, &update);
    let once = doc_text(&receiver);
    apply_update(&receiver, &update);
    let twice = doc_text(&receiver);

    assert_eq!(once, "hello");
    assert_eq!(
        once, twice,
        "applying the same update twice should be idempotent"
    );
}

#[test]
fn crdt_concurrent_edits_converge() {
    let base = base_state("world");

    let doc_a = Doc::with_client_id(10);
    apply_update(&doc_a, &base);

    let doc_b = Doc::with_client_id(11);
    apply_update(&doc_b, &base);

    let update_a = {
        let mut txn = doc_a.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.insert(&mut txn, 0, "hello ");
        txn.encode_update_v1()
    };

    let update_b = {
        let mut txn = doc_b.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.insert(&mut txn, 0, "big ");
        txn.encode_update_v1()
    };

    apply_update(&doc_a, &update_b);
    apply_update(&doc_b, &update_a);

    let final_a = doc_text(&doc_a);
    let final_b = doc_text(&doc_b);

    assert_eq!(
        final_a, final_b,
        "peers should converge to the same content"
    );
    assert!(final_a.contains("hello "));
    assert!(final_a.contains("big "));
    assert!(final_a.contains("world"));
}

#[test]
fn crdt_delete_merge_converges() {
    let base = base_state("hello world");

    let doc_a = Doc::with_client_id(20);
    apply_update(&doc_a, &base);

    let doc_b = Doc::with_client_id(21);
    apply_update(&doc_b, &base);

    let update_a = {
        let mut txn = doc_a.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.remove_range(&mut txn, 0, 6);
        txn.encode_update_v1()
    };

    let update_b = {
        let mut txn = doc_b.transact_mut();
        let text = txn.get_or_insert_text("content");
        text.insert(&mut txn, 6, "big ");
        txn.encode_update_v1()
    };

    apply_update(&doc_a, &update_b);
    apply_update(&doc_b, &update_a);

    let final_a = doc_text(&doc_a);
    let final_b = doc_text(&doc_b);

    assert_eq!(final_a, final_b, "delete/insert merges should converge");
    assert!(final_a.contains("big"));
    assert!(final_a.contains("world"));
    assert!(!final_a.contains("hello "));
}
