CREATE TABLE IF NOT EXISTS shared_invitation (
    id INTEGER NOT NULL PRIMARY KEY,
    sender_id INTEGER NOT NULL REFERENCES user(id),
    recipient_id INTEGER NOT NULL REFERENCES user(id),
    item_type VARCHAR(20) NOT NULL,
    source_id INTEGER NOT NULL,
    item_name VARCHAR(160) NOT NULL,
    payload_json TEXT NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'pending',
    created_at DATETIME NOT NULL,
    responded_at DATETIME
);
CREATE INDEX IF NOT EXISTS ix_shared_invitation_sender_id ON shared_invitation(sender_id);
CREATE INDEX IF NOT EXISTS ix_shared_invitation_recipient_id ON shared_invitation(recipient_id);
CREATE INDEX IF NOT EXISTS ix_shared_invitation_item_type ON shared_invitation(item_type);
CREATE INDEX IF NOT EXISTS ix_shared_invitation_status ON shared_invitation(status);
CREATE INDEX IF NOT EXISTS ix_shared_invitation_recipient_status ON shared_invitation(recipient_id, status);
