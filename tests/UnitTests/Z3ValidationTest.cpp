#include <gtest/gtest.h>

#include <Util/Logger.hpp>
#include <z3++.h>

using namespace z3;
namespace NES {
class Z3ValidationTest : public testing::Test {
  public:
    void SetUp() {
        NES::setupLogging("Z3ValidationTest.log", NES::LOG_DEBUG);
        NES_INFO("Setup Z3ValidationTest test class.");
    }

    void TearDown() {
        NES_INFO("Tear down Z3ValidationTest test class.");
    }

    const size_t buffers_managed = 10;
    const size_t buffer_size = 4 * 1024;
};

/**
   @brief: Demonstration of how Z3 can be used to prove validity of
   De Morgan's Duality Law: {e not(x and y) <-> (not x) or ( not y) }
*/
TEST_F(Z3ValidationTest, deMorganDualityValidation) {
    NES_INFO("De-Morgan Example");

    // create a context
    context c;
    //Create an instance of the solver
    solver s(c);

    //Define boolean constants x and y
    expr x = c.bool_const("x");
    expr y = c.bool_const("y");

    //Define the expression to evaluate
    expr expression = (!(x && y)) == (!x || !y);

    // adding the negation of the expression as a constraint.
    // We try to prove that inverse of this expression is valid (which is obviously false).
    s.add(!expression);

    NES_INFO("Content of the solver: " << s);
    NES_INFO("Statement folding inside z3 solver: " << s.to_smt2());
    ASSERT_EQ(s.check(), unsat);
}

/**
   @brief Validate for <tt>x > 1 and y > 1 that y + x > 1 </tt>.
*/
TEST_F(Z3ValidationTest, evaluateValidBinomialEquation) {

    // create a context
    context c;
    //Create an instance of the solver
    solver s(c);

    //Define int constants
    expr x = c.int_const("x");
    expr y = c.int_const("y");

    //Add equations to
    s.add(x > 1);
    s.add(y > 1);
    s.add(x + y > 1);

    //Assert
    ASSERT_EQ(s.check(), sat);
}

/**
   @brief Validate for <tt>x > 1 and y > 1 that y + x < 1 </tt>.
*/
TEST_F(Z3ValidationTest, evaluateInvalidBinomialEquation) {

    // create a context
    context c;
    //Create an instance of the solver
    solver s(c);

    //Define int constants
    expr x = c.int_const("x");
    expr y = c.int_const("y");

    //Add equations
    s.reset();
    s.add(x > 1);
    s.add(y > 1);
    s.add(x + y < 1);
    //Assert
    ASSERT_EQ(s.check(), unsat);

    //Same equation written using api
    s.reset();
    auto one = c.int_val(1);
    auto xLessThanOne = to_expr(c, Z3_mk_gt(c, x, one));
    auto yLessThanOne = to_expr(c, Z3_mk_gt(c, y, one));
    Z3_ast args[] = {x, y};
    auto xPlusY = to_expr(c, Z3_mk_add(c, 2, args));
    auto xPlusYLessThanOne = to_expr(c, Z3_mk_lt(c, xPlusY, one));

    s.add(xLessThanOne);
    s.add(yLessThanOne);
    s.add(xPlusYLessThanOne);
    //Assert
    ASSERT_EQ(s.check(), unsat);
}

}// namespace NES