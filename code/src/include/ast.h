#pragma once
#include <string>
#include <variant>
#include "table.h"

namespace DB::ast {
	enum class base_t_t { LOGICAL_OP, COMPARISON_OP, ID, NUMERIC, STR, MATH_OP };
	//enum class atom_t_t {  };
	enum class logical_t_t { AND, OR };
	enum class comparison_t_t { EQ, NEQ, LESS, GREATER, LEQ, GEQ, };
	enum class math_t_t { ADD, SUB, MUL, DIV, MOD, };
	const std::string base2str[] = { "LOGICAL_OP", "COMPARISON_OP", "ID", "NUMERIC", "STR", "MATH_OP" };
	const std::string logical2str[] = { "AND", "OR" };
	const std::string comparison2str[] = { "==", "!=", "<", ">", "<=", ">=" };
	const std::string math2str[] = { "+", "-", "*", "/", "%" };

	using RetValue = std::variant<bool, int, std::string>;

	struct BaseExpr {
		BaseExpr(base_t_t base_t) :base_t_(base_t) {}
		virtual ~BaseExpr() = 0;

		const base_t_t  base_t_;
	};

	struct NonAtomExpr : public BaseExpr {
		NonAtomExpr(base_t_t base_t) :BaseExpr(base_t) {}
		virtual ~NonAtomExpr() = 0;
	};

	struct AtomExpr : public BaseExpr {
		AtomExpr(base_t_t base_t) :BaseExpr(base_t) {}
		virtual ~AtomExpr() = 0;
	};

	struct LogicalOpExpr : public NonAtomExpr {
		LogicalOpExpr(logical_t_t logical_t, BaseExpr* left, BaseExpr* right) :
			NonAtomExpr(base_t_t::LOGICAL_OP), logical_t_(logical_t), _left(left), _right(right) {}
		virtual ~LogicalOpExpr();

		const logical_t_t logical_t_;
		BaseExpr* _left;
		BaseExpr* _right;
	};

	struct ComparisonOpExpr : public NonAtomExpr {
		//left,right must be AtomExpr*
		ComparisonOpExpr(comparison_t_t comparison_t, AtomExpr* left, AtomExpr* right) :
			NonAtomExpr(base_t_t::COMPARISON_OP), comparison_t_(comparison_t), _left(left), _right(right) {}
		virtual ~ComparisonOpExpr();

		const comparison_t_t comparison_t_;
		AtomExpr* _left;
		AtomExpr* _right;
	};

	struct MathOpExpr : public AtomExpr {
		MathOpExpr(math_t_t math_t, AtomExpr* left, AtomExpr* right) :
			AtomExpr(base_t_t::MATH_OP), math_t_(math_t), _left(left), _right(right) {}
		virtual ~MathOpExpr();

		const math_t_t math_t_;
		AtomExpr* _left;
		AtomExpr* _right;
	};

	struct IdExpr : public AtomExpr {
		IdExpr(std::string columnName, std::string tableName = std::string()) :
			AtomExpr(base_t_t::ID), _tableName(tableName), _columnName(columnName) {}
		virtual ~IdExpr();

		std::string _tableName;
		std::string _columnName;
	};

	struct NumericExpr : public AtomExpr {
		NumericExpr(int value) :
			AtomExpr(base_t_t::NUMERIC), _value(value) {}
		virtual ~NumericExpr();

		int _value;
	};

	struct StrExpr : public AtomExpr {
		StrExpr(const std::string& value) :
			AtomExpr(base_t_t::STR), _value(value) {}
		virtual ~StrExpr();

		std::string _value;
	};


	enum class op_t_t { PROJECT, FILTER, JOIN, TABLE };
	struct BaseOp {
		BaseOp(op_t_t op_t) :op_t_(op_t) {}
		virtual ~BaseOp() = 0;
		virtual table::VirtualTable getOutput() = 0;

		op_t_t op_t_;
	};

	struct ProjectOp : public BaseOp {
		ProjectOp() :BaseOp(op_t_t::PROJECT) {}
		virtual ~ProjectOp() { delete _source; }
		virtual table::VirtualTable getOutput();

		std::vector<std::string> _names;	//the name of the accordingly element, not sure if useful
		std::vector< ast::AtomExpr*> _elements;	//if empty, $ are used, all columns are needed
		BaseOp* _source;
	};

	struct FilterOp : public BaseOp {
		FilterOp(ast::BaseExpr* whereExpr) :BaseOp(op_t_t::FILTER), _whereExpr(whereExpr) {}
		virtual ~FilterOp() { delete _whereExpr; delete _source; }
		virtual table::VirtualTable getOutput();

		ast::BaseExpr* _whereExpr;
		BaseOp* _source;
	};

	struct JoinOp : public BaseOp {
		JoinOp() :BaseOp(op_t_t::JOIN) {}
		virtual ~JoinOp() {}
		virtual table::VirtualTable getOutput();

		std::vector<BaseOp*> _sources;	//currently suppose all sources are TableOp
		bool isJoin;	//even it's true, not sure if the tables can be joined
	};

	struct TableOp : public BaseOp {
		TableOp(const std::string tableName) : BaseOp(op_t_t::TABLE), _tableName(tableName) {}
		virtual ~TableOp() {}
		virtual table::VirtualTable getOutput();

		std::string _tableName;
	};

	//===========================================================
	//visit functions

	/*
	*output visit, output the ast to the given ostream
	*regardless of validity
	*/
	void outputVisit(const BaseExpr* root, std::ostream &os);

	void outputVisit(const BaseOp* root, std::ostream &os);

	/*
	*check visit, used in parsing phase
	*	1. check sql semantics,
	*		such as TableA.idd == 123 where there doesn't exist idd column in TableA
	*	2. check the type matching,
	*		such as WHERE "wtf" AND TableA.name == 123,
	*		here string "wtf" is not supported by logicalOp AND,
	*		string TableA.name and number 123 don't matched either
	*
	*for any dismatching, throw DB_Exception
	*if passing, it is guaranteed other visit function will never encounter unexcepted cases
	*
	*the visited expr won't be modified at present,
	*may be modified for optimization in the future
	*/

	//check WHERE clause(expression)
	void checkVisit(const BaseExpr* root, const std::string tableName = std::string());

	//check others(expressionAtom)
	void checkVisit(const AtomExpr* root, const std::string tableName = std::string());


	/*
	*vm visit, for vm
	*guarantee no except
	*/
	inline int numericOp(int op1, int op2, math_t_t math_t);
	inline bool comparisonOp(int op1, int op2, comparison_t_t comparison_t);

	//for WHERE clause(expression), used for filtering values of a row
	bool vmVisit(const BaseExpr* root, const table::Row* row);

	//for others(expressionAtom), used for computing math/string expression and data in the specified row
	RetValue vmVisit(const AtomExpr* root, const table::Row* row = nullptr);
}








