# -*- coding: utf-8 -*-
# Copyright European Organization for Nuclear Research (CERN) since 2012
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import datetime
import itertools
import logging
import threading
import weakref
from collections import defaultdict
from decimal import Decimal, localcontext
from typing import TYPE_CHECKING, cast, Any, Callable, Generic, Iterable, Iterator, Dict, List, Optional, Set, Tuple, Type, TypeVar, Union

from sqlalchemy import and_, select

from rucio.common.utils import PriorityQueue
from rucio.common.config import config_get_int, config_get, config_get_bool
from rucio.common.exception import NoDistance, RSEProtocolNotSupported, InvalidRSEExpression
from rucio.core.monitor import MetricManager
from rucio.core.rse import RseCollection, RseData
from rucio.core.rse_expression_parser import parse_expression
from rucio.db.sqla import models
from rucio.db.sqla.session import read_session, transactional_session
from rucio.rse import rsemanager as rsemgr

LoggerFunction = Callable[..., Any]
_Number = Union[float, int]
TN = TypeVar("TN", bound="Node")
TE = TypeVar("TE", bound="Edge")

if TYPE_CHECKING:
    from sqlalchemy.orm import Session

    from typing import Protocol

    class _StateProvider(Protocol):
        @property
        def cost(self) -> _Number:
            ...

        @property
        def enabled(self) -> bool:
            ...


METRICS = MetricManager(module=__name__)
DEFAULT_HOP_PENALTY = 10
INF = float('inf')


class Node(RseData):
    def __init__(self, rse_id: str):
        super().__init__(rse_id)

        self.in_edges = weakref.WeakKeyDictionary()
        self.out_edges = weakref.WeakKeyDictionary()

        self.cost: _Number = 0
        self.capacity: _Number = 10**9
        self.enabled: bool = True
        self.used_for_multihop = False


class Edge(Generic[TN]):
    def __init__(self, src_node: TN, dst_node: TN):
        self._src_node = weakref.ref(src_node)
        self._dst_node = weakref.ref(dst_node)

        self.cost: _Number = 1
        self.capacity: _Number = 10**9
        self.enabled: bool = True

        self.__hash = None

        self.add_to_nodes()

    def add_to_nodes(self):
        self.src_node.out_edges[self.dst_node] = self
        self.dst_node.in_edges[self.src_node] = self

    def remove_from_nodes(self):
        self.src_node.out_edges.pop(self.dst_node, None)
        self.dst_node.in_edges.pop(self.src_node, None)

    @property
    def src_node(self) -> TN:
        node = self._src_node()
        if node is None:
            # This shouldn't happen if the Node list is correctly managed by the Topology object.
            raise ReferenceError("weak reference returned None")
        return node

    @property
    def dst_node(self) -> TN:
        node = self._dst_node()
        if node is None:
            # This shouldn't happen if the Node list is correctly managed by the Topology object.
            raise ReferenceError("weak reference returned None")
        return node

    def __eq__(self, other):
        if not isinstance(other, self.__class__):
            return False
        return self._src_node == other._src_node and self._dst_node == other._dst_node

    def __str__(self):
        return f'{self.src_node}-->{self.dst_node}'

    def __hash__(self):
        if self.__hash is None:
            self.__hash = hash((self.src_node, self.dst_node))
        return self.__hash


class Topology(RseCollection, Generic[TN, TE]):
    """
    Helper private class used to easily fetch topological information for a subset of RSEs.
    """
    def __init__(
            self,
            rse_ids: Optional[Iterable[str]] = None,
            ignore_availability: bool = False,
            node_cls: Type[TN] = Node,
            edge_cls: Type[TE] = Edge,
    ):
        super().__init__(rse_ids=rse_ids, rse_data_cls=node_cls)
        self._edge_cls = edge_cls
        self._edges = {}
        self._edges_loaded = False
        self._multihop_nodes = set()
        self._hop_penalty = DEFAULT_HOP_PENALTY
        self.ignore_availability = ignore_availability

        self._lock = threading.RLock()

    def get_or_create(self, rse_id: str) -> "TN":
        rse_data = self.rse_id_to_data_map.get(rse_id)
        if rse_data is None:
            with self._lock:
                rse_data = self.rse_id_to_data_map.get(rse_id)
                if not rse_data:
                    self.rse_id_to_data_map[rse_id] = rse_data = self._rse_data_cls(rse_id)
                    # A new node added. Edges which were already loaded are probably incomplete now.
                    self._edges_loaded = False
        return rse_data

    def edge(self, src_node: TN, dst_node: TN) -> "Optional[TE]":
        return self._edges.get((src_node, dst_node))

    def get_or_create_edge(self, src_node: TN, dst_node: TN) -> "TE":
        edge = self._edges.get((src_node, dst_node))
        if not edge:
            with self._lock:
                edge = self._edges.get((src_node, dst_node))
                if not edge:
                    self._edges[src_node, dst_node] = edge = self._edge_cls(src_node, dst_node)
        return edge

    def delete_edge(self, src_node: TN, dst_node: TN):
        with self._lock:
            edge = self._edges[src_node, dst_node]
            edge.remove_from_nodes()

    @property
    def multihop_enabled(self) -> bool:
        return True if self._multihop_nodes else False

    @read_session
    def configure_multihop(self, multihop_rse_ids: Optional[Set[str]] = None, *, session: "Session", logger: LoggerFunction = logging.log):
        with self._lock:
            return self._configure_multihop(multihop_rse_ids=multihop_rse_ids, session=session, logger=logger)

    def _configure_multihop(self, multihop_rse_ids: Optional[Set[str]] = None, *, session: "Session", logger: LoggerFunction = logging.log):

        if multihop_rse_ids is None:
            include_multihop = config_get_bool('transfers', 'use_multihop', default=False, expiration_time=600, session=session)
            multihop_rse_expression = config_get('transfers', 'multihop_rse_expression', default='available_for_multihop=true', expiration_time=600, session=session)

            multihop_rse_ids = set()
            if include_multihop:
                try:
                    multihop_rse_ids = {rse['id'] for rse in parse_expression(multihop_rse_expression, session=session)}
                except InvalidRSEExpression:
                    pass
                if multihop_rse_expression and multihop_rse_expression.strip() and not multihop_rse_ids:
                    logger(logging.WARNING, 'multihop_rse_expression is not empty, but returned no RSEs')

        for node in self._multihop_nodes:
            node.used_for_multihop = False

        self._multihop_nodes.clear()

        for rse_id in multihop_rse_ids:
            node = self.get_or_create(rse_id).ensure_loaded(load_columns=True)
            if self.ignore_availability or (node.columns['availability_read'] and node.columns['availability_write']):
                node.used_for_multihop = True
                self._multihop_nodes.add(node)

        self._hop_penalty = config_get_int('transfers', 'hop_penalty', default=DEFAULT_HOP_PENALTY, session=session)
        return self

    @read_session
    def ensure_edges_loaded(self, *, session: "Session"):
        """
        Ensure that all edges are loaded for the (sub-)set of nodes known by this topology object
        """
        if self._edges_loaded:
            return

        with self._lock:
            return self._ensure_edges_loaded(session=session)

    def _ensure_edges_loaded(self, *, session: "Session"):
        stmt = select(
            models.Distance
        ).where(
            and_(
                models.Distance.src_rse_id.in_(self.rse_id_to_data_map.keys()),
                models.Distance.dest_rse_id.in_(self.rse_id_to_data_map.keys()),
            )
        )

        loaded_edges = set()
        for distance in session.execute(stmt).scalars():
            if distance.distance is None:
                continue

            src_node = self[distance.src_rse_id]
            dst_node = self[distance.dest_rse_id]
            edge = self.get_or_create_edge(src_node, dst_node)

            sanitized_dist = int(distance.distance) if distance.distance >= 0 else 0
            edge.cost = sanitized_dist

            loaded_edges.add((src_node, dst_node))

        if len(loaded_edges) != len(self._edges):
            # Remove edges which don't exist in the database anymore
            to_remove = set(self._edges).difference(loaded_edges)
            for src_node, dst_node in to_remove:
                self.delete_edge(src_node, dst_node)

        self._edges_loaded = True


    @read_session
    def reload_distances(self, *, session: "Session"):
        self.ensure_loaded(rse_ids=self.rse_id_to_data_map, load_attributes=True, load_info=True, session=session)
        self._load_inbound_edges(nodes=self.rse_id_to_data_map.values(), session=session)
        self._load_outgoing_edges(nodes=self.rse_id_to_data_map.values(), session=session)
        return self

    @read_session
    def load_all(self, *, session: "Session"):
        pass

    @read_session
    def search_shortest_paths(
            self,
            src_nodes: List[TN],
            dst_node: TN,
            operation_src: str,
            operation_dest: str,
            domain: str,
            limit_dest_schemes: List[str],
            *,
            session: "Session",
    ) -> Dict[TN, List[Dict[str, Any]]]:
        """
        Find the shortest paths from multiple sources towards dest_rse_id.
        """

        for rse in itertools.chain(src_nodes, [dst_node], self._multihop_nodes):
            rse.ensure_loaded(load_attributes=True, load_info=True, session=session)
        self.ensure_edges_loaded(session=session)

        if self._multihop_nodes:
            # Filter out island source RSEs
            nodes_to_find = {node for node in src_nodes if node.out_edges}
        else:
            nodes_to_find = set(src_nodes)

        class _NodeStateProvider:
            _hop_penalty = self._hop_penalty

            def __init__(self, node: TN):
                self.enabled: bool = True
                self.cost: _Number = 0
                if node != dst_node:
                    try:
                        self.cost = int(node.attributes.get('hop_penalty', self._hop_penalty))
                    except ValueError:
                        self.cost = self._hop_penalty

        scheme_missmatch_found = {}

        class _EdgeStateProvider:
            def __init__(self, edge: TE):
                self.edge = edge
                self.chosen_scheme = {}

            @property
            def cost(self) -> _Number:
                return self.edge.cost

            @property
            def enabled(self) -> bool:
                try:
                    matching_scheme = rsemgr.find_matching_scheme(
                        rse_settings_src=self.edge.src_node.info,
                        rse_settings_dest=self.edge.dst_node.info,
                        operation_src=operation_src,
                        operation_dest=operation_dest,
                        domain=domain,
                        scheme=limit_dest_schemes if self.edge.dst_node == dst_node and limit_dest_schemes else None,
                    )
                    self.chosen_scheme = {
                        'source_scheme': matching_scheme[1],
                        'dest_scheme': matching_scheme[0],
                        'source_scheme_priority': matching_scheme[3],
                        'dest_scheme_priority': matching_scheme[2],
                    }
                    return True
                except RSEProtocolNotSupported:
                    scheme_missmatch_found[self.edge.src_node] = True
                    return False

        paths = {dst_node: []}
        for node, distance, _, edge_to_next_hop, edge_state in self.dijkstra_spf(dst_node=dst_node,
                                                                                 nodes_to_find=nodes_to_find,
                                                                                 node_state_provider=_NodeStateProvider,
                                                                                 edge_state_provider=_EdgeStateProvider):
            nh_node = edge_to_next_hop.dst_node
            edge_state = cast(_EdgeStateProvider, edge_state)
            hop = {
                'source_rse': node,
                'dest_rse': nh_node,
                'hop_distance': edge_state.cost,
                'cumulated_distance': distance,
                **edge_state.chosen_scheme,
            }
            paths[node] = [hop] + paths[nh_node]

            nodes_to_find.discard(node)
            if not nodes_to_find:
                # We found the shortest paths to all desired nodes
                break

        result = {}
        for node in src_nodes:
            path = paths.get(node)
            if path is not None:
                result[node] = path
            elif scheme_missmatch_found.get(node):
                result[node] = []
        return result

    def dijkstra_spf(
            self,
            dst_node: TN,
            nodes_to_find: Optional[Set[TN]] = None,
            node_state_provider: "Callable[[TN], _StateProvider]" = lambda x: x,
            edge_state_provider: "Callable[[TE], _StateProvider]" = lambda x: x,
    ) -> "Iterator[Tuple[TN, _Number, _StateProvider, TE, _StateProvider]]":
        """
        Does a Backwards Dijkstra's algorithm: start from destination and follow inbound links to other nodes.
        If multihop is disabled, stop after analysing direct connections to dest_rse.
        If the optional nodes_to_find parameter is set, will restrict search only towards these nodes.
        Otherwise, traverse the graph in integrality.

        Will yield nodes in order of their distance from the destination.
        """

        priority_q = PriorityQueue()
        priority_q[dst_node] = 0
        next_hops: Dict[TN, Tuple[_Number, _StateProvider, Optional[TE], Optional[_StateProvider]]] =\
            {dst_node: (0, node_state_provider(dst_node), None, None)}
        while priority_q:
            node = priority_q.pop()
            node_dist, node_state, edge_to_nh, edge_to_nh_state = next_hops[node]

            if edge_to_nh is not None and edge_to_nh_state is not None:  # skip dst_node
                yield node, node_dist, node_state, edge_to_nh, edge_to_nh_state

            if self._multihop_nodes or edge_to_nh is None:
                # If multihop is disabled, only examine neighbors of dst_node

                for adjacent_node, edge in node.in_edges.items():

                    if nodes_to_find is None or adjacent_node in nodes_to_find or adjacent_node.used_for_multihop:

                        edge_state = edge_state_provider(edge)
                        new_adjacent_dist = node_dist + node_state.cost + edge_state.cost
                        if new_adjacent_dist < next_hops.get(adjacent_node, (INF, ))[0] and edge_state.enabled:
                            adj_node_state = node_state_provider(adjacent_node)
                            next_hops[adjacent_node] = new_adjacent_dist, adj_node_state, edge, edge_state
                            priority_q[adjacent_node] = new_adjacent_dist


class ExpiringObjectCache:
    """
    Thread-safe container which builds and object with the function passed in parameter and
    caches it for the TTL duration.
    """

    def __init__(self, ttl, new_obj_fnc):
        self._lock = threading.Lock()
        self._object = None
        self._creation_time = None
        self._new_obj_fnc = new_obj_fnc
        self._ttl = ttl

    def get(self, logger=logging.log):
        with self._lock:
            if not self._object \
                    or not self._creation_time \
                    or datetime.datetime.utcnow() - self._creation_time > datetime.timedelta(seconds=self._ttl):
                self._object = self._new_obj_fnc()
                self._creation_time = datetime.datetime.utcnow()
                logger(logging.INFO, "Refreshed topology object")
            return self._object

    @METRICS.time_it()
    def karakostas_multicommodity_flow(self, demands, epsilon: Decimal = Decimal('0.1')):
        """
        Compute the maximum multicommodity flow [1]. This corresponds to the most load-balanced way of transferring the given demand.
        The input demand must be in the format {source_node: {destination_node: amount_of_bytes_to_transfer_from_source_to_destination}}
        The resulted solution is fractional: a flow from source_node to destination_node can be split at will over all possible paths.

        Uses the following algorithm. The result will be within (1 - epsilon)**(-3) from the optimal solution.
        [1] Karakostas, G. (2008). Faster approximation schemes for fractional multicommodity flow problems. ACM Transactions on Algorithms (TALG)
        """
        flow = defaultdict(lambda: defaultdict(Decimal))

        total_flow_from_node = defaultdict(Decimal)
        total_flow_to_node = defaultdict(Decimal)
        total_flow_via_edge = defaultdict(Decimal)

        with localcontext() as ctx:
            # We will manipulate very small numbers
            ctx.prec = 50

            num_n_e = len(self.rse_id_to_data_map) + len(self._edges)
            # constants are described (with the accompanying proof) in the linked paper
            delta = (Decimal(1) / pow(Decimal(1) + epsilon, (Decimal(1) - epsilon) / epsilon)) * pow((Decimal(1) - epsilon) / num_n_e, Decimal(1) / epsilon)
            flow_scale_factor = ((Decimal(1) + epsilon) / delta).ln() / (Decimal(1) + epsilon).ln()
            max_iterations = 2 * (Decimal(1) / epsilon) * (num_n_e * (Decimal(1) + epsilon) / (Decimal(1) - epsilon)).ln() / (Decimal(1) + epsilon).ln()

            costs = {}

            class _DictStateProvider:
                enabled = True

                def __init__(self, element: "TN | TE"):
                    self.cost = costs.get(element, None)
                    if self.cost is None:
                        costs[element] = self.cost = delta / element.capacity

            demand_multiplier = 1
            iteration = 0
            dl = Decimal(delta * num_n_e)
            while dl < 1:

                # If the demands are very small compared to edge capacities, this algorithm will require an unreasonable
                # number of iterations to complete. The following mechanism is described in the linked paper and is meant to
                # speed up convergence time by doubling the demands multiple times.
                iteration += 1
                if iteration > max_iterations:
                    iteration = 0
                    demand_multiplier = demand_multiplier * 2

                for dst_node, demand_towards_dst_node in demands.items():
                    demand_towards_dst_node = {n: d * demand_multiplier for n, d in demand_towards_dst_node.items()}

                    any_remaining_demand = True
                    while dl < 1 and any_remaining_demand:
                        sorted_nodes = list(self.dijkstra_spf(dst_node=dst_node,
                                                              nodes_to_find=set(demand_towards_dst_node),
                                                              node_state_provider=_DictStateProvider,
                                                              edge_state_provider=_DictStateProvider))

                        # Find how much flow will pass via every node and edge if we push all demands via the shortest paths.
                        # This may result in an edge or node to get overloaded. overflow_ratio will record by how much it was overloaded
                        overflow_ratio = Decimal(1)
                        inbound_flow_by_node = defaultdict(Decimal)
                        for node, _, _, edge_to_next_hop, _ in reversed(sorted_nodes):
                            next_hop = edge_to_next_hop.dst_node

                            inbound_flow = inbound_flow_by_node[node]
                            # Whatever flow goes out of the current node will have to go into the next hop
                            outbound_flow = inbound_flow + demand_towards_dst_node.get(node, Decimal(0))
                            inbound_flow_by_node[next_hop] = inbound_flow_by_node[next_hop] + outbound_flow
                            # Only accept a fraction of the demand if it risks to overload any edge or node
                            overflow_ratio = max(
                                overflow_ratio,
                                inbound_flow / node.capacity,
                                outbound_flow / edge_to_next_hop.capacity,
                            )
                        overflow_ratio = max(overflow_ratio, inbound_flow_by_node[dst_node] / dst_node.capacity)

                        # Push each demand. If overflow_ratio is bigger than one, only push the fraction of demand which
                        # doesn't overload anything.
                        any_remaining_demand = False
                        sorted_nodes.append((dst_node, None, None, None, None))
                        for node, _, _, edge_to_next_hop, _ in sorted_nodes:
                            desired_demand = demand_towards_dst_node.get(node, Decimal(0))

                            accepted_demand = desired_demand / overflow_ratio
                            accepted_inbound = inbound_flow_by_node[node] / overflow_ratio
                            accepted_outbound = accepted_inbound + accepted_demand

                            if desired_demand > 0:
                                remaining_demand = desired_demand - accepted_demand
                                demand_towards_dst_node[node] = remaining_demand
                                flow[dst_node][node] += accepted_demand
                                if remaining_demand:
                                    any_remaining_demand = True

                            node_volume_increase = 0
                            if accepted_inbound > 0:
                                total_flow_to_node[node] += accepted_inbound

                                node_volume_increase = epsilon * accepted_inbound * costs[node]
                                costs[node] += node_volume_increase / node.capacity

                            edge_volume_increase = 0
                            if edge_to_next_hop is not None and accepted_outbound > 0:
                                total_flow_from_node[node] += accepted_outbound
                                total_flow_via_edge[edge_to_next_hop] += accepted_outbound

                                edge_volume_increase = epsilon * accepted_outbound * costs[edge_to_next_hop]
                                costs[edge_to_next_hop] += edge_volume_increase / edge_to_next_hop.capacity

                            dl += edge_volume_increase + node_volume_increase

            # The computed flow violated edge and node capacity constraints. We need to scale it down.
            lam = Decimal('inf')
            for dst_node, demand_per_src in demands.items():
                for src_node, demand_from_src in demand_per_src.items():
                    scaled_flow = flow[dst_node][src_node] / flow_scale_factor
                    flow[dst_node][src_node] = scaled_flow
                    lam = min(lam, scaled_flow / demand_from_src)
            for node in total_flow_to_node:
                total_flow_to_node[node] /= flow_scale_factor
            for node in total_flow_from_node:
                total_flow_from_node[node] /= flow_scale_factor
            for edge in total_flow_via_edge:
                total_flow_via_edge[edge] /= flow_scale_factor
            print(lam)


@transactional_session
def get_hops(
        source_rse_id: str,
        dest_rse_id: str,
        multihop_rse_ids: Optional[Set[str]] = None,
        limit_dest_schemes: Optional[List[str]] = None,
        *, session: "Session",
):
    """
    Get a list of hops needed to transfer date from source_rse_id to dest_rse_id.
    Ideally, the list will only include one item (dest_rse_id) since no hops are needed.
    :param source_rse_id:       Source RSE id of the transfer.
    :param dest_rse_id:         Dest RSE id of the transfer.
    :param multihop_rse_ids:    List of RSE ids that can be used for multihop. If empty, multihop is disabled.
    :param limit_dest_schemes:  List of destination schemes the matching scheme algorithm should be limited to for a single hop.
    :returns:                   List of hops in the format [{'source_rse_id': source_rse_id, 'source_scheme': 'srm', 'source_scheme_priority': N, 'dest_rse_id': dest_rse_id, 'dest_scheme': 'srm', 'dest_scheme_priority': N}]
    :raises:                    NoDistance
    """
    if not limit_dest_schemes:
        limit_dest_schemes = []

    topology = Topology().configure_multihop(multihop_rse_ids=multihop_rse_ids)
    src_node = topology[source_rse_id]
    dst_node = topology[dest_rse_id]
    shortest_paths = topology.search_shortest_paths(src_nodes=[src_node], dst_node=dst_node,
                                                    operation_src='third_party_copy_read', operation_dest='third_party_copy_write',
                                                    domain='wan', limit_dest_schemes=limit_dest_schemes, session=session)

    path = shortest_paths.get(src_node)
    if path is None:
        raise NoDistance()

    if not path:
        raise RSEProtocolNotSupported()

    return path
