-- Create transaction_queue table
CREATE TABLE transaction_queue (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    account_id TEXT NOT NULL,
    transaction_data JSONB NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    priority INTEGER NOT NULL DEFAULT 0,
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    scheduled_at TIMESTAMPTZ,
    processed_at TIMESTAMPTZ,
    error_message TEXT
);

-- Create indexes
CREATE INDEX idx_transaction_queue_account_id ON transaction_queue(account_id);
CREATE INDEX idx_transaction_queue_status ON transaction_queue(status);
CREATE INDEX idx_transaction_queue_priority_created ON transaction_queue(priority DESC, created_at ASC);
CREATE INDEX idx_transaction_queue_scheduled_at ON transaction_queue(scheduled_at) WHERE scheduled_at IS NOT NULL;

-- Create rate_limits table
CREATE TABLE rate_limits (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    limit_type TEXT NOT NULL,
    max_requests INTEGER NOT NULL,
    window_seconds INTEGER NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create index
CREATE INDEX idx_rate_limits_limit_type ON rate_limits(limit_type);

-- Crate accounts table
CREATE TABLE accounts (
    id TEXT PRIMARY KEY,
    limit_type TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Create update trigger for updated_at
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

CREATE TRIGGER update_transaction_queue_updated_at BEFORE UPDATE
    ON transaction_queue FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_rate_limits_updated_at BEFORE UPDATE
    ON rate_limits FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER update_accpimts_updated_at BEFORE UPDATE
    ON accounts FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();
