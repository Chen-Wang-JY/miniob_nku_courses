
# include "sql/operator/join_logical_operator.h"

JoinLogicalOperator::JoinLogicalOperator(std::unique_ptr<Expression> join_condition)
{
    if (join_condition)
        this->expressions().push_back(std::move(join_condition));
}