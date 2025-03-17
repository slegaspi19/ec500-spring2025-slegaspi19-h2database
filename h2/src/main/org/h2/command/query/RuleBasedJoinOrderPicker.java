package org.h2.command.query;

import org.h2.engine.SessionLocal;
import org.h2.expression.Expression;
import org.h2.expression.ExpressionColumn;
import org.h2.expression.condition.Comparison;
import org.h2.expression.condition.ConditionAndOr;
import org.h2.expression.condition.ConditionAndOrN;
import org.h2.table.TableFilter;

import java.util.*;

/**
 * Determines the best join order by following rules rather than considering every possible permutation.
 */
public class RuleBasedJoinOrderPicker {
    final SessionLocal session;
    final TableFilter[] filters;

    public RuleBasedJoinOrderPicker(SessionLocal session, TableFilter[] filters) {
        this.session = session;
        this.filters = filters;
    }

    public TableFilter[] bestOrder() {
        Map<TableFilter, List<TableFilter>> joinConnections = buildConnectionGraph();

        Optional<TableFilter> startFilter = Arrays.stream(filters).reduce((filterA, filterB) -> filterA.getTable().getRowCountApproximation(session) < filterB.getTable().getRowCountApproximation(session) ? filterA : filterB);

        List<TableFilter> bestOrderList = new ArrayList<>();

        startFilter.ifPresent(filter -> getOrder(filter, joinConnections, bestOrderList, new HashSet<>()));

        return bestOrderList.toArray(new TableFilter[0]);
    }

    private Map<TableFilter, List<TableFilter>> buildConnectionGraph() {
        Map<TableFilter, List<TableFilter>> graph = new HashMap<>();

        for (TableFilter filter : filters) {
            graph.put(filter, new ArrayList<>());
        }

        for (TableFilter filter: filters) {
            Expression fullCondition = filter.getFullCondition();
            if (fullCondition != null) {
                addConnections(fullCondition, graph);
            }
        }

        return graph;
    }

    private void addConnections(Expression expression, Map<TableFilter, List<TableFilter>> graph) {
        if (expression instanceof ConditionAndOr || expression instanceof ConditionAndOrN) {
            for (int i = 0; i < expression.getSubexpressionCount(); i++) {
                addConnections(expression.getSubexpression(i), graph);
            }
        } else if (expression instanceof Comparison) {
            System.out.println("Comparison expression");
            System.out.println(expression);

            Expression left = expression.getSubexpression(0);
            Expression right = expression.getSubexpression(1);

            if (left instanceof  ExpressionColumn && right instanceof ExpressionColumn) {
                TableFilter leftFilter = ((ExpressionColumn) left).getTableFilter();
                TableFilter rightFilter = ((ExpressionColumn) right).getTableFilter();

                System.out.println("Table Filters");
                System.out.println(leftFilter);
                System.out.println(rightFilter);

                if (leftFilter != null && rightFilter != null && leftFilter != rightFilter) {
                    graph.get(leftFilter).add(rightFilter);
                    graph.get(rightFilter).add(leftFilter);
                }
            }
        }
    }

    private void getOrder(TableFilter currentFilter, Map<TableFilter, List<TableFilter>> graph, List<TableFilter> order, Set<TableFilter> visited) {
        visited.add(currentFilter);
        order.add(currentFilter);
        System.out.println("Current Order");
        System.out.println(order);

        List<TableFilter> connections = graph.get(currentFilter);

        if (connections != null) {
            connections.sort(Comparator.comparingLong(connectedFilter -> connectedFilter.getTable().getRowCountApproximation(session)));
            System.out.println("Sorted Connections");
            System.out.println(connections);
            for (TableFilter connection : connections) {
                if (!visited.contains(connection)) {
                    getOrder(connection, graph, order, visited);
                }
            }
        }
    }

}