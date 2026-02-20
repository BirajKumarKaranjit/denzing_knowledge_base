from itlm_db_connector.factory import DatabaseFactory
from itlm_db_connector.connection_pool import ConnectionPool
import logging
from typing import List, Dict, Any

logger = logging.getLogger(__name__)


def get_included_tables_ddl(db_config: Dict[str, Any]) -> List:
    """
    Retrieve DDL for agent-included tables from the connected database.

    Returns format: [ { "tables": [...] }, {}, {}, None ]
    """
    try:
        if not db_config:
            logger.warning("No database config provided for agent")
            return _empty_table_info()

        included_tables = db_config.get("db_included_tables") or []
        if not included_tables:
            logger.warning("No included tables specified in db_config")
            return _empty_table_info()

        database_type = db_config.get("database_type", "").upper()
        connector_cls = DatabaseFactory.get_connector(database_type)
        connector = connector_cls(**db_config)

        table_ddls = []

        with ConnectionPool(connector, max_size=1) as pool:
            connection = pool.get_connection()
            cursor = connection.cursor()

            try:
                for table_name in included_tables:
                    try:
                        ddl = connector.get_ddl(table_name, cursor=cursor)
                        table_ddls.append(
                            {
                                "table_name": table_name,
                                "ddl": ddl,
                            }
                        )
                    except Exception as table_err:
                        logger.warning(
                            "Failed to fetch DDL for table %s: %s",
                            table_name,
                            table_err,
                        )

            finally:
                cursor.close()
                pool.return_connection(connection)

        if not table_ddls:
            return _empty_table_info()

        return format_ddl_list(table_ddls)

    except Exception as e:
        logger.warning("Failed to retrieve table info from database: %s", e)
        return _empty_table_info()


def format_ddl_list(table_info: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Converts list of metadata into a list of single-key dictionaries:
    [{'table_name': 'ddl_string'}, ...]
    """
    return [
        {item['table_name']: item['ddl']}
        for item in table_info
        if 'table_name' in item and 'ddl' in item
    ]

def _empty_table_info() -> List:
    """Standard empty metadata structure."""
    return [{}, {}, {}, None]

# if __name__ == "__main__":
#     # db_config = {
#     #         "host": "CRKGTVE-OR27968",
#     #         "username": "DZ_GENERAL_USR",
#     #         "password": "test@123",
#     #         "private_key": "LS0tLS1CRUdJTiBQUklWQVRFIEtFWS0tLS0tCk1JSUV2UUlCQURBTkJna3Foa2lHOXcwQkFRRUZBQVNDQktjd2dnU2pBZ0VBQW9JQkFRQ3dzV0xwV29xOUlqOUoKUWxablp1MFFESEFDWFVocWltL0d2N0VCMHE1YVJkRm5OeVBRemFFWUZIZUFXeDNSOFJnbk1hZWZ6Y09RbkNxZApqZmRsV2JjUkJsSkJOWmdta01iSzdZZDF1NXUzRmVmQkN1OTdCeERzdElRUjhuU1B1R0JoS0lPZGpBNG0yQWg5CjRsWDNkNkhWVWpjMkEyanNPTnMwTmVNQ2YvRWRVYUlzbmdnRnFJTTRnZ1AvMFFNUlhxNWRORGFVR3pIbzVXeWwKNXVWbzFoSjBuWnpRUmY2dlJ4eHU2Z0RMbVlZSEJ3U0dZUXk0amQ3YW9yblZrSFlIcWxSZnNpdGIvRjkycklKVQpKdFNHS3B0OFdOSzN1VFJaQk1uZXlTUndkZ2ZUMWhWUjFuTUdUa3VUMlhkYkhJQWkydEhEV1p4aXlQaVVxSzRvCm1Uc1V2Y08vQWdNQkFBRUNnZ0VBRDRmZ0FsUG5pdjFXSTduVmYyeEFIUitDdnVwMUlZdytpL0V3ZlZFSDZ6NWsKekJaQUt5dlA0NTUwN0k3VnZhemI2eUlGc2xtelBXUkVxS21nVzhPNWdDRVduTTI1ckhZZ1N4ajZ4YUh5YTNMYwo5bzVnQTJmWkdIMDdlSnBmblh4ZTJGdG55akxUMFowK2pkWVZxVlZXSDdxRXlOWWU1RmlSQi9OT20xc2tsZWQvCjEzWHlEcWZ1TVBsa2Uxb1VyaStjcjlEdVF2V2dNZWxYNVEzRjA5NlRXTzRYTXlISGRNc3o1azVnbGJ4UHBYTzQKRE9CektieE5EWEw2a01tZzNHdjg4NlJaK2N3c3h1L1ZWVEwyVmZvaXY4REFZdjBsWnd5MWlhZlVlNnVyQVExcQpEb2htaUJML3ZwMkZUOVVKVXZBRm0yUzM0UHdJdzJEMUxqNG01dXJmL1FLQmdRRHdSVEt6WHg2KzVVZVlVQTZzCnlwMHRKTDYrWWR1T1AzQzVDY01rUk5GaHFWZ1NIMDNjQzFBWk5NSEtZN2NlMVhqOStKLzlsTVlPNVRBM2VnTC8KTFR4L3dMZE9VTHBJd3FIYVRsbGhwbFJla3NTTjc5bDJKU0wzK3hzWU9nalFQSG1CaTF5ckUxMWhWZmlORlBCeApEVlJMSkJvb1pDbW5CRGV5clZTYko0WHBiUUtCZ1FDOFFxcE5XNWJ6UitSaXVJNDZuMXQvUHdJMW0yQzM3TmtyCkRHNy9aVWlid0V3dUM0U1YrNWc4QlpnQjlmemZhbmVKM3hTVlhVUVprRUV6cERuL2pOMWNIVlZSS1IyVDBoOTgKcTJBRElaYUZ0Z2N2a21rWGFJKzc5c1hpU0dzWk5ORjVMaEdRcE5Bcm05WWVpdk5lTmpJNFYvN0IrcFVGQk9rSgpOR284dTI2eVd3S0JnUUNZK29ZSWZOUzNtSVlZTFVqYXJYcWJwcHluM09pTXprbTRGc1lmam8xOXovT2FQa0kxCml5SnBtaFNWQng0dHpKOW5uT2hJN1hPWlFrV0wzT3lSaWp6TjNtY1h2d0NNbVJleVJsWlVmVEdVc1gvaUcwZHIKR01RRi9lUkhiWlAwK2J3blJoTXZmWG5rSW5mSlkvNmZER3lTRng5c2ZqR2kvR1YySnpRS1FZVGFsUUtCZ0FVcQo4MC9TVHM5NHpyMGpBY3g4S1YvUjAvYXl3REhzVDMzT0JwSCtMc01Qd1VlV1MxcHVvSnd0THlJR3BaMWdaODJpCkVRZVVtdFQwejhWaUliRnhKWFpzMjdXeG9qMDNqR080dEpjQmFnZGJtdERrZlB3R2c4T09INXA1a0c4TnN4NVgKZEdYN0VEZlNQRXQwVnYva0R2YmErNFJKbzZPU29lNzl3RmpRY0ZaM0FvR0FhZkUrWUxtYnV2M0hOa2lqTmg4dwo2YzlKWlhUQUMybUh1M0ltbXhER1d5Y3RhWWYzNXRwV1lMT0NRSlk1eVpaK3RVZHpkaEVvdHh5L0gzV3YzNlA5CkZqWm9NMS9nclVRTDN5S1U3TUpDNFFzYUlqYnVnendTZjNWR1YzUTh1U2Rkdnhrc2NsM2dzdHV3LzYxdEVGalYKeTJIMnVtZHNMaVlSTXJKOGpDRHozRjQ9Ci0tLS0tRU5EIFBSSVZBVEUgS0VZLS0tLS0K",
#     #         "database": "DZ_PUBLIC_DB",
#     #         "schema": "DW_DWH",
#     #         "warehouse": "COMPUTE_WH",
#     #         "role": "DZ_GENERAL",
#     #         "database_type": "snowflake",
#     #         "db_dns": None,
#     #         "db_new": None,
#     #         "db_included_tables": [
#     #         "dwh_d_games",
#     #         "dwh_d_players",
#     #         "dwh_d_player_nicknames",
#     #     ],
#     #     }
#     #
#     # table_info = get_included_tables_ddl(db_config)
#     # print(table_info)
#
#
#
# #for Postgresql
#     db_config = {
#                 "host": "98.82.149.230",
#                 "username": "dz_general_usr",
#                 "password": "PfymGA84rP",
#                 "name": "dz_public_db",
#                 "database": "dz_public_db",
#                 "schema": "dw_dwh",
#                 "role": "dz_general_usr",
#                 "database_type": "postgresql",
#                 "db_dns": None,
#                 "db_new": None,
#                 "db_included_tables": ["dwh_d_games", "dwh_d_players"],
#             }
#     table_info = get_included_tables_ddl(db_config)
#     print(table_info)
