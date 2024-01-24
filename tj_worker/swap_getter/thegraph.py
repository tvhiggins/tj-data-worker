from __future__ import absolute_import

import asyncio
from time import sleep

from gql import Client, gql
from gql.transport import exceptions
from gql.transport.aiohttp import AIOHTTPTransport
from tj_worker.utils import log

logger = log.setup_custom_logger(name=__file__)


class GraphAPI(object):
    """Request data from thegraph.com

    Example Usage:
        with thegraph.GraphAPI() as g:
            data = g.get_pair(id=pair_str)

    """

    def __init__(self):
        base_url = "https://api.thegraph.com/subgraphs/name/traderjoe-xyz/exchange"

        # Select your transport with a defined url endpoint
        transport = AIOHTTPTransport(url=base_url)

        # Create a GraphQL client using the defined transport
        self.client = Client(transport=transport, fetch_schema_from_transport=True)
        self.retries = 0

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        pass

    def get_token(self, id: str):
        """Get Token data

        Args:
            id (str): id for token to query

        Returns:
            list: Token data
        """
        query = gql(
            """
                        query FetchTokenbyID($tokenFilter: Token_filter)
                        {
                          tokens(where: $tokenFilter) {
                            id
                            symbol
                            name
                          }
                        }
        """
        )
        token_filter = {"id": id}
        variable_values = {"tokenFilter": token_filter}
        data = self._send_request(query=query, variable_values=variable_values)
        if "tokens" in data:
            return data["tokens"]
        else:
            return data

    def get_transactions(self, block_number: int):
        """Get Transaction data data

        Args:
            block_number (int): minimum block_number to start query

        Returns:
            list: Transaction data
        """
        query = gql(
            """
                        query FetchTransactbyBlockNumber($blockNumberFilter: Transaction_filter)
                        {
                        transactions(where: $blockNumberFilter
                        orderBy:blockNumber
                        orderDirection: asc
                        )
                        { 	id
                                timestamp
                                    blockNumber
                                swaps{
                                        id
                                        amountUSD
                                        amount0In
                                        amount0Out
                                        amount1In
                                        amount1Out
                                        pair{	id
                                            }
                                }
                        }
                        }
        """
        )
        block_number_filter = {"blockNumber_gte": str(block_number)}
        variable_values = {"blockNumberFilter": block_number_filter}
        data = self._send_request(query=query, variable_values=variable_values)
        if "transactions" in data:
            return data["transactions"]
        else:
            return None

    def get_pair(self, id: str) -> dict:
        """Get Pair data

        Args:
            id (str): id for pair to query

        Returns:
            dict: Pair data
        """
        query = gql(
            """
                        query FetchPairbyID($pairFilter: Pair_filter)
                        {
                        pairs(where: $pairFilter)
                        { 	id
                            name
                            token0 {
                                id
                                symbol
                                name
                                }
                            token1 {
                                id
                                symbol
                                name
                                }
                        }
                        }
        """
        )
        pair_filter = {"id": id}
        variable_values = {"pairFilter": pair_filter}
        data = self._send_request(query=query, variable_values=variable_values)
        if "pairs" in data:
            return data["pairs"][0]
        else:
            return None

    def _send_request(self, query: str = None, variable_values: dict = None, max_retries: int = 3):
        """Send request to endpoint

        Args:
            query (str, optional): Query string for request. Defaults to None.
            variable_values (dict, optional): variables to populate query with. Defaults to None.
            max_retries (int, optional): Number of retry attempts. Defaults to 3.

        Returns:
            dict[str, any]: API response
        """

        def retry():
            self.retries += 1
            if self.retries > max_retries:
                exit(1)
            else:
                logger.info("Retrying Request....")
                sleep(30)
                return self._send_request(
                    query=query,
                    variable_values=variable_values,
                    max_retries=max_retries,
                )

        # Make the request
        try:
            response = self.client.execute(query, variable_values=variable_values)
        except exceptions.TransportQueryError as e:
            logger.error(e)
            return retry()
        except asyncio.CancelledError as e:
            logger.error(e)
            return retry()
        except asyncio.exceptions.TimeoutError as e:
            logger.error(e)
            return retry()

        self.retries = 0
        return response
