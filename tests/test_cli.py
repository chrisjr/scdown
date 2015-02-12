from click.testing import CliRunner

from scdown.scripts.cli import cli


def test_cli_basic():
    runner = CliRunner()
    result = runner.invoke(cli, [])
    assert result.exit_code == 2
    assert result.output.startswith("Usage:")
