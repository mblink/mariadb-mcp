"""
Unit tests for the server's contract with fastmcp and the mcp SDK. No database.

The integration suite drives the tools through an in-memory fastmcp client
against a real MariaDB; these pin the library-facing behaviour on their own, so
a fastmcp/mcp upgrade that changes tool registration, input schemas, result
serialization, error mapping or the stateless streamable-HTTP transport (what
production runs: FASTMCP_STATELESS_HTTP=true, --transport http) fails here
without Docker. `_execute_query` is mocked; the pool is a stand-in, so nothing
connects.
"""
import unittest
from unittest.mock import AsyncMock, MagicMock, patch

import fastmcp
import mcp.types
from fastmcp.client import Client
from fastmcp.utilities.tests import asgi_client, temporary_settings

from src.server import MariaDBServer

EXPECTED_TOOLS = {
    'list_databases', 'list_tables', 'get_table_schema',
    'get_table_schema_with_relations', 'execute_sql', 'create_database',
}


def _server(read_only=False):
    server = MariaDBServer()
    server.is_read_only = read_only
    server.pool = MagicMock(name='pool')  # register_tools refuses a None pool
    server.register_tools()
    return server


class _MockedQueryCase(unittest.IsolatedAsyncioTestCase):
    read_only = False

    async def asyncSetUp(self):
        self.server = _server(self.read_only)
        patcher = patch.object(self.server, '_execute_query', new_callable=AsyncMock)
        self.query = patcher.start()
        self.addCleanup(patcher.stop)


class TestToolRegistration(_MockedQueryCase):

    async def test_registers_exactly_the_six_tools(self):
        async with Client(self.server.mcp) as client:
            names = {tool.name for tool in await client.list_tools()}
        self.assertEqual(names, EXPECTED_TOOLS)

    async def test_register_tools_requires_a_pool(self):
        with self.assertRaises(RuntimeError):
            MariaDBServer().register_tools()

    async def test_input_schemas_mark_required_and_optional_parameters(self):
        async with Client(self.server.mcp) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}

        def required(name):
            return set(tools[name].inputSchema.get('required', []))

        self.assertEqual(required('list_databases'), set())
        self.assertEqual(required('list_tables'), {'database_name'})
        self.assertEqual(required('get_table_schema'), {'database_name', 'table_name'})
        self.assertEqual(
            required('get_table_schema_with_relations'), {'database_name', 'table_name'})
        self.assertEqual(required('execute_sql'), {'sql_query', 'database_name'})
        self.assertIn('parameters', tools['execute_sql'].inputSchema['properties'])

    async def test_tool_descriptions_come_from_docstrings(self):
        async with Client(self.server.mcp) as client:
            tools = {tool.name: tool for tool in await client.list_tools()}
        self.assertIn('table-level comment', tools['get_table_schema'].description)
        self.assertIn('read-only', tools['execute_sql'].description)


class TestToolCalls(_MockedQueryCase):

    async def test_list_returns_structured_data(self):
        self.query.return_value = [{'Database': 'BondLink'}, {'Database': 'mysql'}]
        async with Client(self.server.mcp) as client:
            result = await client.call_tool('list_databases', {})
        self.assertFalse(result.is_error)
        self.assertEqual(result.data, ['BondLink', 'mysql'])

    async def test_execute_sql_passes_parameters_as_a_tuple(self):
        self.query.return_value = [{'id': 7}]
        async with Client(self.server.mcp) as client:
            result = await client.call_tool('execute_sql', {
                'database_name': 'BondLink',
                'sql_query': 'SELECT id FROM t WHERE id = %s',
                'parameters': [7],
            })
        self.assertEqual(result.data, [{'id': 7}])
        args, kwargs = self.query.await_args
        self.assertEqual(args[1] if len(args) > 1 else kwargs.get('params'), (7,))

    async def test_missing_required_argument_is_a_tool_error(self):
        async with Client(self.server.mcp) as client:
            with self.assertRaises(fastmcp.exceptions.ToolError):
                await client.call_tool('list_tables', {})
        self.query.assert_not_awaited()

    async def test_server_exception_surfaces_as_tool_error(self):
        self.query.side_effect = RuntimeError('boom')
        async with Client(self.server.mcp) as client:
            with self.assertRaises(fastmcp.exceptions.ToolError):
                await client.call_tool('list_databases', {})

    async def test_raise_on_error_false_returns_is_error_result(self):
        self.query.side_effect = RuntimeError('boom')
        async with Client(self.server.mcp) as client:
            result = await client.call_tool('list_databases', {}, raise_on_error=False)
        self.assertTrue(result.is_error)
        self.assertIsInstance(result.content[0], mcp.types.TextContent)


class _Reached(Exception):
    """Raised by the stand-in pool: the query got past the read-only check."""


class TestReadOnlyGate(unittest.IsolatedAsyncioTestCase):
    """Exercises the real `_execute_query` prefix check, stopping at the pool."""

    async def asyncSetUp(self):
        self.server = _server(read_only=True)
        self.server.pool.acquire.side_effect = _Reached

    async def _run(self, sql):
        await self.server._execute_query(sql)

    async def test_read_statements_pass_the_gate(self):
        for sql in ('SELECT 1', 'SHOW TABLES', 'DESC t', 'DESCRIBE t',
                    'EXPLAIN SELECT 1', 'EXPLAIN EXTENDED SELECT 1',
                    '/* c */ explain format=json select 1', '-- c\nSELECT 1'):
            with self.subTest(sql=sql):
                # _execute_query wraps unexpected pool errors in RuntimeError.
                with self.assertRaises(RuntimeError) as caught:
                    await self._run(sql)
                self.assertIsInstance(caught.exception.__cause__, _Reached)

    async def test_write_statements_are_refused(self):
        for sql in ('INSERT INTO t VALUES (1)', 'UPDATE t SET a = 1', 'DELETE FROM t',
                    'DROP TABLE t', 'CREATE DATABASE x', 'ANALYZE TABLE t',
                    '/* SELECT */ DELETE FROM t'):
            with self.subTest(sql=sql), self.assertRaises(PermissionError):
                await self._run(sql)

    async def test_refusal_reaches_the_client_as_tool_error(self):
        async with Client(self.server.mcp) as client:
            with self.assertRaises(fastmcp.exceptions.ToolError):
                await client.call_tool('create_database', {'database_name': 'x'})


class TestStatelessHttpTransport(_MockedQueryCase):
    """The production transport: streamable HTTP with no server-side sessions."""

    async def test_tools_list_and_call_over_stateless_http(self):
        self.query.return_value = [{'Database': 'BondLink'}]
        with temporary_settings(stateless_http=True):
            async with asgi_client(self.server.mcp, transport='http') as client:
                names = {tool.name for tool in await client.list_tools()}
                result = await client.call_tool('list_databases', {})
        self.assertEqual(names, EXPECTED_TOOLS)
        self.assertEqual(result.data, ['BondLink'])

    async def test_each_request_is_independent_when_stateless(self):
        self.query.return_value = [{'Database': 'BondLink'}]
        with temporary_settings(stateless_http=True):
            async with asgi_client(self.server.mcp, transport='http') as client:
                for _ in range(3):
                    self.assertEqual(
                        (await client.call_tool('list_databases', {})).data, ['BondLink'])
        self.assertEqual(self.query.await_count, 3)


if __name__ == '__main__':
    unittest.main()
