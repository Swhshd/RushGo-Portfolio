"""Regenerate into a temporary directory and compare every checked-in stub."""
import argparse
from pathlib import Path
import subprocess
import tempfile


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--write", action="store_true", help="explicitly update generated stubs")
    args = parser.parse_args()
    backend = Path(__file__).resolve().parents[1]
    mismatches = []
    with tempfile.TemporaryDirectory(prefix="rushgo-proto-") as tmp:
        output = Path(tmp)
        for source in sorted((backend / "proto").rglob("*.proto")):
            relative = source.relative_to(backend / "proto")
            short = relative.parts[0] in ("admission", "notification")
            logical = relative.as_posix() if short else "proto/" + relative.as_posix()
            target = output / relative.parts[0]
            target.mkdir()
            command = ["protoc", "-I", str(backend / "proto" if short else backend),
                       "--go_out=" + str(target), "--go_opt=paths=source_relative",
                       "--go-grpc_out=" + str(target), "--go-grpc_opt=paths=source_relative", logical]
            subprocess.run(command, cwd=backend, check=True)
            for suffix in (".pb.go", "_grpc.pb.go"):
                generated = target / (logical.removesuffix(".proto") + suffix)
                stored = backend / "pkg/proto" / (relative.as_posix().removesuffix(".proto") + suffix)
                text = generated.read_text(encoding="utf-8")
                if args.write:
                    stored.write_text(text, encoding="utf-8", newline="\n")
                elif not stored.exists() or stored.read_text(encoding="utf-8") != text:
                    mismatches.append(str(stored.relative_to(backend)))
    if mismatches:
        raise SystemExit("PROTO_MISMATCH: " + ", ".join(mismatches))
    print("PROTO_CONSISTENCY=PASS" if not args.write else "PROTO_GENERATED")


if __name__ == "__main__":
    main()
