"""ADR0180 diagnostic-only connection substitution; not yet runner-registered."""
import hashlib
import ipaddress
import json
import os
import stat
from dataclasses import dataclass, field
from pathlib import Path
from types import MappingProxyType

READ_ONLY_OPTIONS = "-c default_transaction_read_only=on"
IDENTITY_SQL = """SELECT current_user, current_database(),
 current_setting('default_transaction_read_only')='on',
 current_setting('transaction_read_only')='on',
 pg_has_role(current_user,'pg_read_all_stats','USAGE')"""


def identity(role, database):
    return hashlib.sha256(json.dumps([role, database], separators=(",", ":")).encode()).hexdigest()


@dataclass(frozen=True, repr=False)
class ConnectionSpec:
    parameters: dict = field(repr=False)
    identity_sha256: str

    def __post_init__(self):
        object.__setattr__(self, "parameters", MappingProxyType(dict(self.parameters)))

    def __repr__(self):
        return "ConnectionSpec(<protected>)"


def specification(value, *, expected_database, expected_identity, ca_path, expected_endpoint):
    keys = {"host", "port", "dbname", "user", "password", "sslmode", "sslrootcert"}
    if not isinstance(value, dict) or set(value) != keys:
        raise ValueError("Exact diagnostic connection fields required")
    try:
        if not isinstance(value["host"], str):
            raise TypeError("String endpoint required")
        address = ipaddress.IPv4Address(value["host"])
    except (ValueError, TypeError, ipaddress.AddressValueError):
        raise ValueError("Private diagnostic endpoint required") from None
    if (not address.is_private or type(value["port"]) is not int or not 1 <= value["port"] <= 65535
            or (value["host"], value["port"]) != expected_endpoint
            or value["dbname"] != expected_database or value["sslmode"] != "verify-full"
            or value["sslrootcert"] != str(ca_path) or not Path(ca_path).is_absolute()
            or not isinstance(value["user"], str) or not value["user"]
            or not isinstance(value["password"], str) or not value["password"]
            or identity(value["user"], value["dbname"]) != expected_identity):
        raise ValueError("Diagnostic identity, endpoint or verified TLS differs")
    parameters = dict(value, autocommit=True, connect_timeout=5,
                      prepare_threshold=None, options=READ_ONLY_OPTIONS)
    return ConnectionSpec(parameters, expected_identity)


def load_bundle(path, *, owned_directory, expected_sha256, expected_database, expected_identity, expected_endpoint, expected_ca_sha256):
    """Read only an exact owner-only bundle and CA inside the owned directory."""
    directory, path = Path(owned_directory), Path(path)
    if os.name != "posix" or directory.is_symlink() or path.is_symlink():
        raise ValueError("POSIX owned diagnostic bundle required")
    directory = directory.resolve(strict=True)
    if path.parent.resolve(strict=True) != directory or stat.S_IMODE(directory.stat().st_mode) != 0o700:
        raise ValueError("Exclusive diagnostic directory required")
    if directory.stat().st_uid != os.geteuid():
        raise ValueError("Diagnostic directory owner differs")
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as handle:
        metadata = os.fstat(handle.fileno())
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_uid != os.geteuid() or not 0 < metadata.st_size <= 16384):
            raise ValueError("Owner-only bounded diagnostic bundle required")
        raw = handle.read(16385)
    if hashlib.sha256(raw).hexdigest() != expected_sha256:
        raise ValueError("Diagnostic bundle binding differs")
    value = json.loads(raw)
    ca = directory / "diagnostic-ca.pem"
    if ca.is_symlink():
        raise ValueError("Diagnostic CA must not be a symlink")
    descriptor = os.open(ca, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(descriptor, "rb") as handle:
        metadata = os.fstat(handle.fileno())
        if (not stat.S_ISREG(metadata.st_mode) or metadata.st_nlink != 1 or stat.S_IMODE(metadata.st_mode) != 0o600
                or metadata.st_uid != os.geteuid() or not 0 < metadata.st_size <= 262144):
            raise ValueError("Owner-only bounded diagnostic CA required")
        certificate = handle.read(262145)
        if hashlib.sha256(certificate).hexdigest() != expected_ca_sha256:
            raise ValueError("Diagnostic CA binding differs")
        if b"BEGIN CERTIFICATE" not in certificate:
            raise ValueError("Diagnostic CA certificate required")
    return specification(value, expected_database=expected_database,
                         expected_identity=expected_identity, ca_path=ca, expected_endpoint=expected_endpoint)


class DiagnosticConnect:
    """Replace exactly the data connection; preserve every other connection call."""
    def __init__(self, original, source_dsn, spec):
        self.original, self.source_dsn, self.spec = original, source_dsn, spec
        self.connections_created = 0

    def __call__(self, conninfo="", **kwargs):
        if conninfo != self.source_dsn:
            return self.original(conninfo, **kwargs)
        if kwargs != {"autocommit": True} or self.connections_created:
            raise ValueError("Exactly one diagnostic data connection required")
        # Never retry a failed connection or fall back to the application account.
        self.connections_created += 1
        connection = self.original(**self.spec.parameters)
        try:
            with connection.transaction():
                connection.execute("SET TRANSACTION READ ONLY")
                connection.execute("SET LOCAL statement_timeout='100ms'")
                row = connection.execute(IDENTITY_SQL).fetchone()
                if (not row or len(row) != 5 or identity(row[0], row[1]) != self.spec.identity_sha256
                        or any(flag is not True for flag in row[2:])):
                    raise ValueError("Read-only diagnostic identity or visibility differs")
            return connection
        except BaseException:
            connection.close()
            raise


class _Driver:
    def __init__(self, original, connect):
        self._original, self.connect = original, connect

    def __getattr__(self, name):
        return getattr(self._original, name)


def install(module, source_dsn, spec):
    """Alter the isolated observer module, never the process-wide psycopg driver."""
    connect = DiagnosticConnect(module.psycopg.connect, source_dsn, spec)
    module.psycopg = _Driver(module.psycopg, connect)
    return connect
