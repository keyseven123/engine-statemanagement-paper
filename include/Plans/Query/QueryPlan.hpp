#ifndef NES_INCLUDE_PLANS_QUERY_HPP_
#define NES_INCLUDE_PLANS_QUERY_HPP_
#include <Nodes/Util/Iterators/BreadthFirstNodeIterator.hpp>
#include <Operators/OperatorNode.hpp>
#include <Plans/Query/QueryId.hpp>
#include <Plans/Query/QuerySubPlanId.hpp>
#include <memory>
#include <set>
#include <vector>

namespace NES {

class Stream;
typedef std::shared_ptr<Stream> StreamPtr;

class OperatorNode;
typedef std::shared_ptr<OperatorNode> OperatorNodePtr;

class QueryPlan;
typedef std::shared_ptr<QueryPlan> QueryPlanPtr;

class SourceLogicalOperatorNode;
typedef std::shared_ptr<SourceLogicalOperatorNode> SourceLogicalOperatorNodePtr;

class SinkLogicalOperatorNode;
typedef std::shared_ptr<SinkLogicalOperatorNode> SinkLogicalOperatorNodePtr;

/**
 * @brief The query plan encapsulates a set of operators and provides a set of utility functions.
 */
class QueryPlan {
  public:
    /**
     * @brief Creates a new query plan with a query id, a query sub plan id and a vector of root operators.
     * @param queryId :  the query id
     * @param querySubPlanId : the query sub-plan id
     * @param rootOperators : vector of root Operators
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<OperatorNodePtr> rootOperators);

    /**
     * @brief Creates a new query plan with a root operator.
     * @param rootOperator The root operator usually a source operator.
     * @return a pointer to the query plan.
     */
    static QueryPlanPtr create(OperatorNodePtr rootOperator);

    /**
     * @brief Creates a new query plan without and operator.
     * @return a pointer to the query plan
     */
    static QueryPlanPtr create();

    /**
     * @brief Get all source operators
     * @return vector of logical source operators
     */
    std::vector<SourceLogicalOperatorNodePtr> getSourceOperators();

    /**
     * @brief Get all sink operators
     * @return vector of logical sink operators
     */
    std::vector<SinkLogicalOperatorNodePtr> getSinkOperators();

    /**
     * @brief Appends an operator to the query plan and make the new operator as root.
     * @param operatorNode : new operator
     */
    void appendOperatorAsNewRoot(OperatorNodePtr operatorNode);

    /**
     * @brief Pre-pend the pre-existing operator to the leaf of the query plan.
     * Note: Pre-existing operators are the operators that are already part of another
     * query plan and are getting moved to another query plan or are the system created network sink or source operators
     * Note: this operation will add the pre-existing operator without assigning it a new Id.
     * @param operatorNode
     */
    void prependOperatorAsLeafNode(OperatorNodePtr operatorNode);

    /**
     * @brief Returns string representation of the query.
     */
    std::string toString();

    /**
     * @brief Get the list of root operators for the query graph.
     * NOTE: in certain stages the sink operators might not be the root operators
     * @return
     */
    std::vector<OperatorNodePtr> getRootOperators();

    /**
     * add subQuery's rootnode into the current node for merging purpose.
     * Note: improves this when we have to due with multi-root use case.
     * @param root
     */
    void addRootOperator(OperatorNodePtr root);

    /**
     * remove the an operator from the root operator list.
     * @param root
     */
    void removeAsRootOperator(OperatorNodePtr root);

    /**
     * @brief Get all the operators of a specific type
     * @return returns a vector of operators
     */
    template<class T>
    std::vector<std::shared_ptr<T>> getOperatorByType() {
        // Find all the nodes in the query plan
        NES_DEBUG("QueryPlan: Get all  nodes in the query plan.");
        std::vector<std::shared_ptr<T>> operators;
        // Maintain a list of visited nodes as there are multiple root nodes
        std::set<uint64_t> visitedOpIds;
        NES_DEBUG("QueryPlan: Iterate over all root nodes to find the operator.");
        for (auto rootOperator : rootOperators) {
            auto bfsIterator = BreadthFirstNodeIterator(rootOperator);
            for (auto itr = bfsIterator.begin(); itr != bfsIterator.end(); ++itr) {
                auto visitingOp = (*itr)->as<OperatorNode>();
                if (visitedOpIds.find(visitingOp->getId()) != visitedOpIds.end()) {
                    // skip rest of the steps as the node found in already visited node list
                    continue;
                }
                NES_DEBUG("QueryPlan: Inserting operator in collection of already visited node.");
                visitedOpIds.insert(visitingOp->getId());
                if (visitingOp->instanceOf<T>()) {
                    NES_DEBUG("QueryPlan: Found leaf node. Adding to the collection of window nodes.");
                    operators.push_back(visitingOp->as<T>());
                }
            }
        }
        return operators;
    }

    /**
    * @brief Get all the leaf operators in the query plan (leaf operator is the one without any child)
     * Note: in certain stages the source operators might not be Leaf operators
    * @return returns a vector of leaf operators
    */
    std::vector<OperatorNodePtr> getLeafOperators();

    /**
     * Find if the operator with the input Id exists in the plan.
     * Note: This method only check if there exists another operator with same Id or not.
     * Note: The system generated operators are ignored from this check.
     * @param operatorId: Id of the operator
     * @return true if the operator exists else false
     */
    bool hasOperatorWithId(uint64_t operatorId);

    /**
     * @brief Get operator node with input id if present
     * @param operatorId : the input operator id
     * @return operator with the input id
     */
    OperatorNodePtr getOperatorWithId(uint64_t operatorId);

    /**
     * Set the query Id for the plan
     * @param queryId
     */
    void setQueryId(QueryId queryId);

    /**
     * Get the queryId for the plan
     * @return query Id of the plan
     */
    const QueryId getQueryId() const;

    /**
     * @brief get query sub plan id
     * @return return query sub plan id
     */
    QuerySubPlanId getQuerySubPlanId();

    /**
     * @brief Set query sub plan id
     * @param querySubPlanId : input query sub plan id
     */
    void setQuerySubPlanId(uint64_t querySubPlanId);

  private:
    /**
     * @brief Creates a new query plan with a query id, a query sub plan id and a vector of root operators.
     * @param queryId :  the query id
     * @param querySubPlanId : the query sub-plan id
     * @param rootOperators : vector of root Operators
     */
    QueryPlan(QueryId queryId, QuerySubPlanId querySubPlanId, std::vector<OperatorNodePtr> rootOperators);

    /**
     * @brief initialize query plan with a root operator
     * @param rootOperator
     */
    QueryPlan(OperatorNodePtr rootOperator);

    /**
     * @brief initialize an empty query plan
     */
    QueryPlan();

    std::vector<OperatorNodePtr> rootOperators;
    QueryId queryId;
    QuerySubPlanId querySubPlanId;
};
}// namespace NES
#endif//NES_INCLUDE_PLANS_QUERY_HPP_
