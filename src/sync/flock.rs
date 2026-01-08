//! File locking utilities for sync coordination with agents.
//!
//! Uses flock(2) to coordinate access to files between the sync client
//! and agents like Bartleby that hold files open while editing.

use std::fs::File;
use std::io;
use std::os::unix::io::AsRawFd;
use std::path::Path;
use std::time::{Duration, Instant};

/// Default timeout for acquiring flock before proceeding anyway
pub const FLOCK_TIMEOUT: Duration = Duration::from_secs(30);

/// Interval between flock retry attempts
pub const FLOCK_RETRY_INTERVAL: Duration = Duration::from_millis(100);

/// Result of attempting to acquire a file lock
#[derive(Debug)]
pub enum FlockResult {
    /// Lock was acquired successfully
    Acquired(FlockGuard),
    /// Timed out waiting for lock - proceeding anyway
    Timeout,
}

/// RAII guard that releases the flock when dropped
#[derive(Debug)]
pub struct FlockGuard {
    file: File,
}

impl Drop for FlockGuard {
    fn drop(&mut self) {
        // SAFETY: flock with LOCK_UN is safe on a valid file descriptor
        unsafe {
            libc::flock(self.file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}

impl FlockGuard {
    /// Get a reference to the underlying file
    pub fn file(&self) -> &File {
        &self.file
    }
}

/// Attempt to acquire an exclusive lock on a file, retrying until timeout.
///
/// This is used before writing to a file to ensure no agent is actively editing it.
/// If the lock cannot be acquired within FLOCK_TIMEOUT, proceeds anyway with a warning.
///
/// # Arguments
/// * `path` - Path to the file to lock
/// * `timeout` - Maximum time to wait for lock (defaults to FLOCK_TIMEOUT)
///
/// # Returns
/// * `FlockResult::Acquired` - Lock acquired, write can proceed safely
/// * `FlockResult::Timeout` - Timed out, proceeding anyway (agent's problem)
pub async fn try_flock_exclusive(
    path: &Path,
    timeout: Option<Duration>,
) -> io::Result<FlockResult> {
    let timeout = timeout.unwrap_or(FLOCK_TIMEOUT);
    let start = Instant::now();

    loop {
        // Reopen the file each iteration to catch inode changes if the file
        // is deleted and recreated while we're waiting for the lock.
        let file = match File::open(path) {
            Ok(f) => f,
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                // File doesn't exist, no lock needed
                return Err(e);
            }
            Err(e) => return Err(e),
        };

        let fd = file.as_raw_fd();

        // Try non-blocking exclusive lock
        // SAFETY: flock is safe on a valid file descriptor
        let result = unsafe { libc::flock(fd, libc::LOCK_EX | libc::LOCK_NB) };

        if result == 0 {
            return Ok(FlockResult::Acquired(FlockGuard { file }));
        }

        // Check errno to determine how to handle the failure
        let errno = io::Error::last_os_error();
        let raw_errno = errno.raw_os_error().unwrap_or(0);

        if raw_errno == libc::EINTR {
            // Interrupted by signal - retry immediately without sleeping
            continue;
        }

        if raw_errno != libc::EWOULDBLOCK && raw_errno != libc::EAGAIN {
            // Unexpected error - return immediately, don't spin for 30s
            return Err(errno);
        }

        // EWOULDBLOCK/EAGAIN: Lock held by another process - check timeout then retry
        if start.elapsed() >= timeout {
            tracing::warn!(
                ?path,
                elapsed = ?start.elapsed(),
                "flock timeout after {:?}, proceeding anyway",
                timeout
            );
            return Ok(FlockResult::Timeout);
        }

        // Wait and retry
        tokio::time::sleep(FLOCK_RETRY_INTERVAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[tokio::test]
    async fn test_flock_acquires_on_unlocked_file() {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();

        let result = try_flock_exclusive(file.path(), Some(Duration::from_secs(1)))
            .await
            .unwrap();

        assert!(matches!(result, FlockResult::Acquired(_)));
    }

    #[tokio::test]
    async fn test_flock_returns_not_found_for_missing_file() {
        let result =
            try_flock_exclusive(Path::new("/nonexistent/file"), Some(Duration::from_secs(1))).await;

        assert!(result.is_err());
        assert_eq!(result.unwrap_err().kind(), io::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn test_flock_times_out_when_locked() {
        use std::os::unix::io::AsRawFd;

        let mut file = NamedTempFile::new().unwrap();
        file.write_all(b"test content").unwrap();

        // Hold an exclusive lock
        let lock_file = File::open(file.path()).unwrap();
        unsafe {
            libc::flock(lock_file.as_raw_fd(), libc::LOCK_EX);
        }

        // Try to acquire with short timeout
        let start = Instant::now();
        let result = try_flock_exclusive(file.path(), Some(Duration::from_millis(200)))
            .await
            .unwrap();

        assert!(matches!(result, FlockResult::Timeout));
        assert!(start.elapsed() >= Duration::from_millis(200));

        // Release the lock
        unsafe {
            libc::flock(lock_file.as_raw_fd(), libc::LOCK_UN);
        }
    }
}
