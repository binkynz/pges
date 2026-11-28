//! The auto traits of the public error types and handles are part of the API: losing `Send` or `Sync` is a
//! breaking change. Every error is `Error + Send + Sync + 'static`, so it boxes into `anyhow::Error` or
//! `Box<dyn Error + Send + Sync>`, and the handles an application shares across tasks are `Send` (and `Sync`
//! where they are shared by reference). These checks compile or fail; they run nothing.

use std::error::Error;

fn error<E: Error + Send + Sync + 'static>() {}

fn send<T: Send>() {}

fn send_sync<T: Send + Sync>() {}

#[test]
fn core_errors_are_send_sync_and_static() {
    use pges::core::{
        category, event, frame, high_water, interval, lsn, metadata, non_empty, store, tls,
    };

    error::<category::CategoryError>();
    error::<event::CodecError>();
    error::<frame::FrameError>();
    error::<high_water::NoticeError>();
    error::<interval::IntervalError>();
    error::<lsn::ParseLsnError>();
    error::<metadata::MetadataError>();
    error::<non_empty::EmptyError>();
    error::<store::StoreNameError>();
    error::<tls::TlsError>();
}

#[test]
fn postgres_errors_are_send_sync_and_static() {
    use pges::postgres::{
        admin, append, delete, error, freeze, limit, load, migrate, read, retention, store,
        unit_of_work,
    };

    error::<admin::RegisterCategoryError>();
    error::<admin::ChangeBucketError>();
    error::<admin::MaintainError>();
    error::<admin::PartitionHeadroomError>();
    error::<append::AppendError>();
    error::<delete::DeleteStreamError>();
    error::<error::DbError>();
    error::<freeze::FreezeError>();
    error::<freeze::PartitionAgesError>();
    error::<limit::SetWriterLimitsError>();
    error::<limit::WriterRoleError>();
    error::<load::LoadError>();
    error::<load::SnapshotError>();
    error::<migrate::MigrateError>();
    error::<read::ReadError>();
    error::<retention::SetRetentionError>();
    error::<retention::SetDefaultRetentionError>();
    error::<retention::RetentionError>();
    error::<retention::PruneLogError>();
    error::<store::OpenError>();
    error::<store::RegisterError>();
    error::<unit_of_work::TransactionError>();
}

#[test]
fn postgres_handles_are_send() {
    use pges::postgres::{admin, store, unit_of_work};

    send_sync::<store::Store>();
    send_sync::<admin::Admin>();
    send::<unit_of_work::UnitOfWork<'static>>();
    send::<unit_of_work::WrappedUnitOfWork<'static>>();
}

#[cfg(feature = "subscribe")]
#[test]
fn subscribe_errors_and_handles() {
    use pges::subscribe::{backfill, batch, handler, subscription, waker};

    error::<backfill::BackfillError>();
    error::<batch::DecodeError>();
    error::<handler::HandlerError>();
    error::<subscription::SubscriptionNameError>();
    error::<subscription::SubscribeError>();
    error::<waker::WakeError>();

    send_sync::<batch::Batch>();
    send::<subscription::Subscription>();
    send_sync::<subscription::SubscriptionHandle>();
    send::<waker::Wakers>();
}

#[cfg(feature = "wake-grpc")]
#[test]
fn grpc_waker_errors() {
    use pges::subscribe::waker::grpc;

    error::<grpc::EndpointError>();
    error::<grpc::Unreachable>();
    error::<grpc::Rejected>();
    send::<grpc::GrpcWaker>();
}

#[cfg(feature = "sequencer-outbox")]
#[test]
fn sequencer_errors_and_handles() {
    use pges::sequencer::{outbox, sequencer, source, watch};

    error::<sequencer::RunError>();
    error::<source::SourceError>();

    send::<sequencer::Sequencer<outbox::OutboxSource>>();
    send::<sequencer::Sequencer<Box<dyn source::DynSource>>>();
    send_sync::<watch::Watch>();
}

#[cfg(feature = "source-logical")]
#[test]
fn logical_source_errors_and_handles() {
    use pges::sequencer::{logical, sequencer};

    error::<logical::ResequenceError>();
    error::<logical::SlotLost>();
    send::<sequencer::Sequencer<logical::LogicalSource>>();
}

#[cfg(feature = "watch-grpc")]
#[test]
fn watch_server_errors_and_handles() {
    use pges::sequencer::grpc;

    error::<grpc::ServeError>();
    send_sync::<grpc::WatchService>();
}

#[cfg(feature = "replication")]
#[test]
fn replication_errors_and_handles() {
    use pges::replication::{config, connection, error, name, pgoutput, stream};

    error::<config::ConfigError>();
    error::<error::ConnectError>();
    error::<error::TlsError>();
    error::<error::CertificateError>();
    error::<error::CommandError>();
    error::<error::StreamError>();
    error::<error::ServerError>();
    error::<name::NameError>();
    error::<pgoutput::DecodeError>();

    send_sync::<config::Config>();
    send::<connection::ReplicationConnection>();
    send::<stream::ReplicationStream>();
}
