/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.NestedColumn;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.planner.ExpressionExtractor;
import com.facebook.presto.sql.planner.PlanNodeIdAllocator;
import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.planner.SymbolAllocator;
import com.facebook.presto.sql.planner.SymbolsExtractor;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.Assignments;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SimplePlanRewriter;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.UnnestNode;
import com.facebook.presto.sql.tree.ComparisonExpression;
import com.facebook.presto.sql.tree.DefaultExpressionTraversalVisitor;
import com.facebook.presto.sql.tree.DereferenceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionRewriter;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.SubscriptExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.sql.analyzer.ExpressionAnalyzer.getExpressionTypes;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

public class PushDownDereferenceExpression
        implements PlanOptimizer
{
    private final Metadata metadata;
    private final SqlParser sqlParser;

    public PushDownDereferenceExpression(Metadata metadata, SqlParser sqlParser)
    {
        this.metadata = requireNonNull(metadata, "metadata is null");
        this.sqlParser = requireNonNull(sqlParser, "sqlparser is null");
    }

    @Override
    public PlanNode optimize(PlanNode plan, Session session, Map<Symbol, Type> types, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
    {
        return SimplePlanRewriter.rewriteWith(new Optimizer(session, metadata, sqlParser, symbolAllocator, idAllocator), plan, new HashMap<>());
    }

    private static class Optimizer
            extends SimplePlanRewriter<Map<Expression, DereferenceInfo>>
    {
        private final Session session;
        private final SqlParser sqlParser;
        private final SymbolAllocator symbolAllocator;
        private final PlanNodeIdAllocator idAllocator;
        private final Metadata metadata;

        private Optimizer(Session session, Metadata metadata, SqlParser sqlParser, SymbolAllocator symbolAllocator, PlanNodeIdAllocator idAllocator)
        {
            this.session = session;
            this.sqlParser = sqlParser;
            this.metadata = metadata;
            this.symbolAllocator = symbolAllocator;
            this.idAllocator = idAllocator;
        }

        @Override
        public PlanNode visitAggregation(AggregationNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            extractDereferenceInfos(node).forEach(expressionInfoMap::putIfAbsent);

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            Map<Symbol, AggregationNode.Aggregation> aggregations = new HashMap<>();
            for (Map.Entry<Symbol, AggregationNode.Aggregation> symbolAggregationEntry : node.getAggregations().entrySet()) {
                Symbol symbol = symbolAggregationEntry.getKey();
                AggregationNode.Aggregation oldAggregation = symbolAggregationEntry.getValue();
                AggregationNode.Aggregation newAggregation = new AggregationNode.Aggregation(ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), oldAggregation.getCall()), oldAggregation.getSignature(), oldAggregation.getMask());
                aggregations.put(symbol, newAggregation);
            }
            return new AggregationNode(
                    idAllocator.getNextId(),
                    child,
                    aggregations,
                    node.getGroupingSets(),
                    node.getStep(),
                    node.getHashSymbol(),
                    node.getGroupIdSymbol());
        }

        @Override
        public PlanNode visitFilter(FilterNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            extractDereferenceInfos(node).forEach(expressionInfoMap::putIfAbsent);

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            Expression predicate = ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), node.getPredicate());
            return new FilterNode(idAllocator.getNextId(), child, predicate);
        }

        @Override
        public PlanNode visitProject(ProjectNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            //  parentDereferenceInfos is used to find out passThroughSymbol. we will only pass those symbols that are needed by upstream
            List<DereferenceInfo> parentDereferenceInfos = expressionInfoMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());
            extractDereferenceInfos(node).forEach(expressionInfoMap::putIfAbsent);

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            List<Symbol> passThroughSymbols = getUsedDereferenceInfo(node.getOutputSymbols(), parentDereferenceInfos).stream().filter(DereferenceInfo::isFromValidSource).map(DereferenceInfo::getSymbol).collect(Collectors.toList());

            Assignments.Builder assignmentsBuilder = Assignments.builder();
            for (Map.Entry<Symbol, Expression> entry : node.getAssignments().entrySet()) {
                assignmentsBuilder.put(entry.getKey(), ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), entry.getValue()));
            }
            assignmentsBuilder.putIdentities(passThroughSymbols);
            ProjectNode newProjectNode = new ProjectNode(idAllocator.getNextId(), child, assignmentsBuilder.build());
            return newProjectNode;
        }

        @Override
        public PlanNode visitTableScan(TableScanNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            List<DereferenceInfo> usedDereferenceInfo = getUsedDereferenceInfo(node.getOutputSymbols(), expressionInfoMap.values());
            if (!usedDereferenceInfo.isEmpty()) {
                usedDereferenceInfo.forEach(DereferenceInfo::doesFromValidSource);
                Map<Symbol, Expression> assignmentMap = usedDereferenceInfo.stream().collect(Collectors.toMap(DereferenceInfo::getSymbol, DereferenceInfo::getDereference));
                return new ProjectNode(idAllocator.getNextId(), node, Assignments.builder().putAll(assignmentMap).putIdentities(node.getOutputSymbols()).build());
                //return mergeProjectWithTableScan(usedDereferenceInfo, node);
            }
            return node;
        }

        private class ToNestedColumnContext
        {
            boolean recursive;
            ImmutableList.Builder<String> builder;

            ToNestedColumnContext()
            {
                recursive = true;
                builder = ImmutableList.builder();
            }
        }

        private Optional<NestedColumn> toNestedColumn(DereferenceInfo dereferenceInfo, TableScanNode tableScanNode)
        {
            ToNestedColumnContext columnContext = new ToNestedColumnContext();
            Map<Symbol, ColumnHandle> assignments = tableScanNode.getAssignments();
            new DefaultExpressionTraversalVisitor<Void, ToNestedColumnContext>()
            {
                @Override
                protected Void visitSubscriptExpression(SubscriptExpression node, ToNestedColumnContext context)
                {
                    // TODO process subscript expression.
                    context.recursive = false;
                    return null;
                }

                @Override
                protected Void visitDereferenceExpression(DereferenceExpression node, ToNestedColumnContext context)
                {
                    process(node.getBase(), context);
                    if (context.recursive) {
                        context.builder.add(node.getField().getValue());
                    }
                    return null;
                }

                @Override
                protected Void visitSymbolReference(SymbolReference node, ToNestedColumnContext context)
                {
                    Symbol baseName = Symbol.from(node);
                    Preconditions.checkArgument(assignments.containsKey(baseName), "base [%s] doesn't exist in assignments [%s]", baseName, assignments);
                    ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableScanNode.getTable(), assignments.get(baseName));
                    context.builder.add(columnMetadata.getName());
                    return null;
                }
            }.process(dereferenceInfo.getDereference(), columnContext);
            List<String> names = columnContext.builder.build();
            return names.size() > 0 ? Optional.of(new NestedColumn(names, dereferenceInfo.getType())) : Optional.empty();
        }

        private static class DereferenceReplacer1
                extends ExpressionRewriter<Void>
        {
            private final Map<Expression, Symbol> map;

            DereferenceReplacer1(Map<Expression, Symbol> map)
            {
                this.map = map;
            }

            @Override
            public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
            {
                if (map.containsKey(node)) {
                    return map.get(node).toSymbolReference();
                }
                return treeRewriter.defaultRewrite(node, context);
            }
        }

        Expression getDereferenceExpressionFromNestedColumnMetadata(ColumnMetadata columnMetadata, Map<String, Symbol> columnNameToSymbol)
        {
            String name = columnMetadata.getName();
            Iterable<String> strings = Splitter.on('.').trimResults().omitEmptyStrings().split(name);
            Expression result = null;

            for (String part : strings) {
                if (result == null) {
                    Preconditions.checkArgument(columnNameToSymbol.containsKey(part), "first element(base) doesn't exist in the map");
                    result = columnNameToSymbol.get(part).toSymbolReference();
                }
                else {
                    result = new DereferenceExpression(result, new Identifier(part));
                }
            }

            return result;
        }

        public PlanNode mergeProjectWithTableScan(List<DereferenceInfo> dereferenceInfos, TableScanNode tableScanNode)
        {
            // get nested columns. All dereference expression will have a mapping nested column. array[1].task will return array
            List<NestedColumn> nestedColumns = dereferenceInfos.stream().map(dereferenceInfo -> toNestedColumn(dereferenceInfo, tableScanNode)).filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());

            // get column handlers. Not all nested columns will have a mapping columnhandle. e.g, msg.foo might return for msg.foo.a and msg.foo.b
            Map<String, ColumnHandle> nestedColumnHandles = metadata.getNestedColumnHandles(session, tableScanNode.getTable(), nestedColumns);

            ImmutableMap.Builder<Symbol, ColumnHandle> columnHandleBuilder = ImmutableMap.builder();

            // Use to replace expression in original dereference expression
            ImmutableMap.Builder<Expression, Symbol> symbolExpressionBuilder = ImmutableMap.builder();

            // get column name to symbol mapping from original tableScan
            ImmutableMap.Builder<String, Symbol> columnNameToSymbolBuilder = ImmutableMap.builder();
            for (Map.Entry<Symbol, ColumnHandle> entry : tableScanNode.getAssignments().entrySet()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableScanNode.getTable(), entry.getValue());
                columnNameToSymbolBuilder.put(columnMetadata.getName(), entry.getKey());
            }
            Map<String, Symbol> columnNameToSymbol = columnNameToSymbolBuilder.build();

            for (Map.Entry<String, ColumnHandle> columnHandleEntry : nestedColumnHandles.entrySet()) {
                ColumnMetadata columnMetadata = metadata.getColumnMetadata(session, tableScanNode.getTable(), columnHandleEntry.getValue());
                Symbol symbol = symbolAllocator.newSymbol(columnMetadata.getName(), columnMetadata.getType());
                symbolExpressionBuilder.put(getDereferenceExpressionFromNestedColumnMetadata(columnMetadata, columnNameToSymbol), symbol);
                columnHandleBuilder.put(symbol, columnHandleEntry.getValue());
            }
            ImmutableMap<Symbol, ColumnHandle> nestedColumnsMap = columnHandleBuilder.build();
            ImmutableMap<Expression, Symbol> symbolExpressionMap = symbolExpressionBuilder.build();

            List<Symbol> originalOutputSymbols = tableScanNode.getOutputSymbols();
            List<Symbol> outputSymbols = ImmutableList.<Symbol>builder().addAll(originalOutputSymbols).addAll(nestedColumnsMap.keySet()).build();
            Map<Symbol, ColumnHandle> assignments = ImmutableMap.<Symbol, ColumnHandle>builder().putAll(tableScanNode.getAssignments()).putAll(nestedColumnsMap).build();
            TableScanNode newTableScan = new TableScanNode(idAllocator.getNextId(), tableScanNode.getTable(), outputSymbols, assignments, tableScanNode.getLayout(), tableScanNode.getCurrentConstraint(), tableScanNode.getOriginalConstraint());

            DereferenceReplacer1 dereferenceReplacer1 = new DereferenceReplacer1(symbolExpressionMap);
            Assignments.Builder assignmentBuilder = Assignments.builder();
            for (DereferenceInfo dereferenceInfo : dereferenceInfos) {
                Expression expression = ExpressionTreeRewriter.rewriteWith(dereferenceReplacer1, dereferenceInfo.getDereference());
                assignmentBuilder.put(dereferenceInfo.getSymbol(), expression);
            }
            return new ProjectNode(idAllocator.getNextId(), newTableScan, assignmentBuilder.putIdentities(originalOutputSymbols).build());
        }

        @Override
        public PlanNode visitJoin(JoinNode joinNode, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            extractDereferenceInfos(joinNode).forEach(expressionInfoMap::putIfAbsent);

            PlanNode leftNode = context.rewrite(joinNode.getLeft(), expressionInfoMap);
            PlanNode rightNode = context.rewrite(joinNode.getRight(), expressionInfoMap);

            List<JoinNode.EquiJoinClause> equiJoinClauses = joinNode.getCriteria().stream()
                    .map(JoinNode.EquiJoinClause::toExpression)
                    .map(expr -> ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), expr))
                    .map(this::getEquiJoinClause)
                    .collect(Collectors.toList());

            Optional<Expression> joinFilter = joinNode.getFilter().map(expression -> ExpressionTreeRewriter.rewriteWith(new DereferenceReplacer(expressionInfoMap), expression));

            return new JoinNode(
                    joinNode.getId(),
                    joinNode.getType(),
                    leftNode,
                    rightNode,
                    equiJoinClauses,
                    ImmutableList.<Symbol>builder().addAll(leftNode.getOutputSymbols()).addAll(rightNode.getOutputSymbols()).build(),
                    joinFilter,
                    joinNode.getLeftHashSymbol(),
                    joinNode.getRightHashSymbol(),
                    joinNode.getDistributionType());
        }

        @Override
        public PlanNode visitUnnest(UnnestNode node, RewriteContext<Map<Expression, DereferenceInfo>> context)
        {
            Map<Expression, DereferenceInfo> expressionInfoMap = context.get();
            List<DereferenceInfo> parentDereferenceInfos = expressionInfoMap.entrySet().stream().map(Map.Entry::getValue).collect(Collectors.toList());

            PlanNode child = context.rewrite(node.getSource(), expressionInfoMap);

            List<Symbol> passThroughSymbols = getUsedDereferenceInfo(child.getOutputSymbols(), parentDereferenceInfos).stream().filter(DereferenceInfo::isFromValidSource).map(DereferenceInfo::getSymbol).collect(Collectors.toList());
            UnnestNode unnestNode = new UnnestNode(idAllocator.getNextId(), child, ImmutableList.<Symbol>builder().addAll(node.getReplicateSymbols()).addAll(passThroughSymbols).build(), node.getUnnestSymbols(), node.getOrdinalitySymbol());

            List<Symbol> unnestSymbols = unnestNode.getUnnestSymbols().entrySet().stream().flatMap(entry -> entry.getValue().stream()).collect(Collectors.toList());
            List<DereferenceInfo> dereferenceExpressionInfos = getUsedDereferenceInfo(unnestSymbols, expressionInfoMap.values());
            if (!dereferenceExpressionInfos.isEmpty()) {
                dereferenceExpressionInfos.forEach(DereferenceInfo::doesFromValidSource);
                Map<Symbol, Expression> assignmentMap = dereferenceExpressionInfos.stream().collect(Collectors.toMap(DereferenceInfo::getSymbol, DereferenceInfo::getDereference));
                return new ProjectNode(idAllocator.getNextId(), unnestNode, Assignments.builder().putAll(assignmentMap).putIdentities(unnestNode.getOutputSymbols()).build());
            }
            return unnestNode;
        }

        private List<DereferenceInfo> getUsedDereferenceInfo(List<Symbol> symbols, Collection<DereferenceInfo> dereferenceExpressionInfos)
        {
            Set<Symbol> symbolSet = symbols.stream().collect(Collectors.toSet());
            return dereferenceExpressionInfos.stream().filter(dereferenceExpressionInfo -> symbolSet.contains(dereferenceExpressionInfo.getBaseSymbol())).collect(Collectors.toList());
        }

        private JoinNode.EquiJoinClause getEquiJoinClause(Expression expression)
        {
            checkArgument(expression instanceof ComparisonExpression, "expression [%s] is not equal expression", expression);
            ComparisonExpression comparisonExpression = (ComparisonExpression) expression;
            return new JoinNode.EquiJoinClause(Symbol.from(comparisonExpression.getLeft()), Symbol.from(comparisonExpression.getRight()));
        }

        private Type extractType(Expression expression)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, metadata, sqlParser, symbolAllocator.getTypes(), expression, emptyList());
            return expressionTypes.get(NodeRef.of(expression));
        }

        private DereferenceInfo getDereferenceInfo(Expression expression)
        {
            Type type = extractType(expression);
            Symbol symbol = symbolAllocator.newSymbol(expression, extractType(expression));
            Symbol base = Iterables.getOnlyElement(SymbolsExtractor.extractAll(expression));
            return new DereferenceInfo(expression, symbol, type, base);
        }

        private List<Expression> extractDereference(Expression expression)
        {
            ImmutableList.Builder<Expression> builder = ImmutableList.builder();
            new DefaultExpressionTraversalVisitor<Void, ImmutableList.Builder<Expression>>()
            {
                @Override
                protected Void visitDereferenceExpression(DereferenceExpression node, ImmutableList.Builder<Expression> context)
                {
                    context.add(node);
                    return null;
                }
            }.process(expression, builder);
            return builder.build();
        }

        private Map<Expression, DereferenceInfo> extractDereferenceInfos(PlanNode node)
        {
            return ExpressionExtractor.extractExpressionsNonRecursive(node).stream()
                    .flatMap(expression -> extractDereference(expression).stream())
                    .filter(expression -> !SymbolsExtractor.extractAll(expression).isEmpty())
                    .map(this::getDereferenceInfo)
                    .collect(Collectors.toMap(DereferenceInfo::getDereference, Function.identity(), (exp1, exp2) -> exp1));
        }
    }

    private static class DereferenceReplacer
            extends ExpressionRewriter<Void>
    {
        private final Map<Expression, DereferenceInfo> map;

        DereferenceReplacer(Map<Expression, DereferenceInfo> map)
        {
            this.map = map;
        }

        @Override
        public Expression rewriteDereferenceExpression(DereferenceExpression node, Void context, ExpressionTreeRewriter<Void> treeRewriter)
        {
            if (map.containsKey(node) && map.get(node).isFromValidSource()) {
                return map.get(node).getSymbol().toSymbolReference();
            }
            return treeRewriter.defaultRewrite(node, context);
        }
    }

    private static class DereferenceInfo
    {
        // e.g. for dereference expression msg.foo[1].bar, base is "msg", newSymbol is new assigned symbol to replace this dereference expression
        private final Expression dereferenceExpression;
        private final Symbol symbol;
        private final Type type;
        private final Symbol baseSymbol;

        // fromValidSource is used to check whether the dereference expression is from either TableScan or Unnest
        // it will be false for following node therefore we won't rewrite:
        // Project[expr_1 := "max_by"."field1"]
        // - Aggregate[max_by := "max_by"("expr", "app_rating")] => [max_by:row(field0 varchar, field1 varchar)]
        private boolean fromValidSource;

        public DereferenceInfo(Expression dereferenceExpression, Symbol symbol, Type type, Symbol baseSymbol)
        {
            this.dereferenceExpression = requireNonNull(dereferenceExpression);
            this.symbol = requireNonNull(symbol);
            this.baseSymbol = requireNonNull(baseSymbol);
            this.type = requireNonNull(type);
            this.fromValidSource = false;
        }

        public Symbol getSymbol()
        {
            return symbol;
        }

        public Symbol getBaseSymbol()
        {
            return baseSymbol;
        }

        public Expression getDereference()
        {
            return dereferenceExpression;
        }

        public boolean isFromValidSource()
        {
            return fromValidSource;
        }

        public void doesFromValidSource()
        {
            fromValidSource = true;
        }

        @Override
        public String toString()
        {
            return String.format("(%s, %s, %s, %s)", dereferenceExpression, symbol, type, baseSymbol);
        }

        public Type getType()
        {
            return type;
        }
    }
}
