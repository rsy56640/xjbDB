#include "include/ast.h"
#include "include/vm.h"

#define DEBUG

namespace DB::ast {

	BaseExpr::~BaseExpr() { }

	NonAtomExpr::~NonAtomExpr() { }

	AtomExpr::~AtomExpr() { }

	LogicalOpExpr::~LogicalOpExpr()
	{
		delete _left;
		delete _right;
	}

	ComparisonOpExpr::~ComparisonOpExpr()
	{
		delete _left;
		delete _right;
	}

	MathOpExpr::~MathOpExpr()
	{
		delete _left;
		delete _right;
	}

	IdExpr::~IdExpr() { }

	NumericExpr::~NumericExpr() { }

	StrExpr::~StrExpr() { }

	BaseOp::~BaseOp() { }

	//===========================================================
	//DML
	table::VirtualTable ProjectOp::getOutput()
	{
		table::VirtualTable table = _source->getOutput();
		return table::vm_->projection(table, _elements);
	}

	table::VirtualTable FilterOp::getOutput()
	{
		table::VirtualTable table = _source->getOutput();
		return table::vm_->sigma(table, _whereExpr);
	}

	table::VirtualTable JoinOp::getOutput()
	{
		if (isJoin)
			return table::vm_->join(_sources[0]->getOutput(), _sources[1]->getOutput(), true);
		table::VirtualTable table = _sources[0]->getOutput();
		for (size_t i = 1; i < _sources.size(); ++i)
		{
			table = table::vm_->join(table, _sources[i]->getOutput(), false);
		}
		return table;
	}

	table::VirtualTable TableOp::getOutput()
	{
		return table::vm_->scanTable(this->_tableName);
	}


	//===========================================================
	//visit functions

	//output visit
	void _outputVisit(const BaseExpr* root, std::ostream& os, size_t indent)
	{
		base_t_t base_t = root->base_t_;
		os << std::string(indent * 2, '-') << base2str[int(base_t)] << "  ";
		++indent;
		switch (base_t)
		{
		case base_t_t::LOGICAL_OP:
		{
			const LogicalOpExpr* logicalPtr = static_cast<const LogicalOpExpr*>(root);
			os << logical2str[int(logicalPtr->logical_t_)] << std::endl;
			_outputVisit(logicalPtr->_left, os, indent);
			_outputVisit(logicalPtr->_right, os, indent);
		}
		break;
		case base_t_t::COMPARISON_OP:
		{
			const ComparisonOpExpr* comparisonPtr = static_cast<const ComparisonOpExpr*>(root);
			os << comparison2str[int(comparisonPtr->comparison_t_)] << std::endl;
			_outputVisit(comparisonPtr->_left, os, indent);
			_outputVisit(comparisonPtr->_right, os, indent);
		}
		break;
		case base_t_t::MATH_OP:
		{
			const MathOpExpr* mathPtr = static_cast<const MathOpExpr*>(root);
			os << math2str[int(mathPtr->math_t_)] << std::endl;
			_outputVisit(mathPtr->_left, os, indent);
			_outputVisit(mathPtr->_right, os, indent);
		}
		break;
		case base_t_t::ID:
		{
			const IdExpr* idPtr = static_cast<const IdExpr*>(root);
			os << idPtr->_tableName << "." << idPtr->_columnName << std::endl;
		}
		break;
		case base_t_t::NUMERIC:
		{
			const NumericExpr* numericPtr = static_cast<const NumericExpr*>(root);
			os << numericPtr->_value << std::endl;
		}
		break;
		case base_t_t::STR:
		{
			const StrExpr* strPtr = static_cast<const StrExpr*>(root);
			os << strPtr->_value << std::endl;
		}
		break;
		default:
			os << "Unexpected base_t" << std::endl;
			break;
		}
	}

	void outputVisit(const BaseExpr* root, std::ostream& os)
	{
		_outputVisit(root, os, 2);
	}

	void _outputVisit(const BaseOp* root, std::ostream &os, size_t indent)
	{
		op_t_t op_t = root->op_t_;
		std::string prefix = std::string(indent * 2, '-');
		++indent;
		os << prefix;
		switch (op_t)
		{
		case DB::ast::op_t_t::PROJECT:
		{
			const ProjectOp* projectOp = static_cast<const ProjectOp*>(root);
			os << "Project on ";
			if (projectOp->_elements.empty())
				os << "ALL" << std::endl;
			else
			{
				os << std::endl;
				for (auto expr : projectOp->_elements)
				{
					_outputVisit(expr, std::cout, indent);
				}
			}
			os << prefix << "From" << std::endl;
			_outputVisit(projectOp->_source, os, indent);
		}
			break;
		case DB::ast::op_t_t::FILTER:
		{
			const FilterOp* filterOp = static_cast<const FilterOp*>(root);
			os << "Filter with " << std::endl;
			_outputVisit(filterOp->_whereExpr, std::cout, indent);
			os << prefix << "From" << std::endl;
			_outputVisit(filterOp->_source, os, indent);
		}
			break;
		case DB::ast::op_t_t::JOIN:
		{
			const JoinOp* joinOp = static_cast<const JoinOp*>(root);
			if (joinOp->_sources.size() > 1)
			{
				if (joinOp->isJoin)
					os << "Join on " << std::endl;
				else
					os << "Cartesian product on" << std::endl;
			}
			else
				--indent;
			
			for (auto source : joinOp->_sources)
				_outputVisit(source, os, indent);
		}
			break;
		case DB::ast::op_t_t::TABLE:
		{
			const TableOp* tableOp = static_cast<const TableOp*>(root);
			os << "Table " << tableOp->_tableName << std::endl;
		}
			break;
		default:
			break;
		}
	}

	void outputVisit(const BaseOp* root, std::ostream &os)
	{
		_outputVisit(root, os, 0);
	}

	//check visit
	//using CheckValue = std::tuple< bool, base_t_t, RetValue>;	//<hasIdExpr, type, value>
	enum class check_t_t { INT, STRING, BOOL };
	const std::string check2str[] = { "INT", "STRING", "BOOL" };

	check_t_t _checkVisit(const BaseExpr* root, const std::string& tableName)
	{
		base_t_t base_t = root->base_t_;
		switch (base_t)
		{
		case base_t_t::LOGICAL_OP:
		{
			const LogicalOpExpr* logicalPtr = static_cast<const LogicalOpExpr*>(root);
			check_t_t left_t = _checkVisit(logicalPtr->_left, tableName);
			check_t_t right_t = _checkVisit(logicalPtr->_right, tableName);
			std::string op = logical2str[int(logicalPtr->logical_t_)];  //used for exception info

			//RetValue here must be bool
			if (left_t != check_t_t::BOOL || right_t != check_t_t::BOOL)
				throw std::string("operation '" + op + "' only support bool oprands");
			return check_t_t::BOOL;
		}
		case base_t_t::COMPARISON_OP:
		{
			const ComparisonOpExpr* comparisonPtr = static_cast<const ComparisonOpExpr*>(root);
			check_t_t left_t = _checkVisit(comparisonPtr->_left, tableName);
			check_t_t right_t = _checkVisit(comparisonPtr->_right, tableName);
			std::string op = comparison2str[int(comparisonPtr->comparison_t_)];

			//RetValue here must be int / str
			if (left_t == right_t)
			{
				if (left_t == check_t_t::STRING)
					if (comparisonPtr->comparison_t_ != comparison_t_t::EQ && comparisonPtr->comparison_t_ != comparison_t_t::NEQ)
						throw std::string("unsupported operation '" + op + "' on strings");
				return check_t_t::BOOL;
			}
			else
			{
				throw std::string("'" + op + "' on mismatched type " + check2str[int(left_t)] + ", " + check2str[int(right_t)]);
			}

		}
		case base_t_t::MATH_OP:
		case base_t_t::ID:
		case base_t_t::NUMERIC:
		case base_t_t::STR:
		default:
			throw std::string("wrong type " + base2str[int(base_t)] + " for WHERE clause");
		}
	}

	void checkVisit(const BaseExpr* root, const std::string tableName)
	{
#ifdef DEBUG
		if (!root)
			throw std::string("BaseExpr* root is nullptr");
#endif // DEBUG
		_checkVisit(root, tableName);
	}

	check_t_t _checkVisit(const AtomExpr* root, const std::string& tableName)
	{
		base_t_t base_t = root->base_t_;
		switch (base_t)
		{
		case base_t_t::MATH_OP:
		{
			const MathOpExpr* mathPtr = static_cast<const MathOpExpr*>(root);
			check_t_t left_t = _checkVisit(mathPtr->_left, tableName);
			check_t_t right_t = _checkVisit(mathPtr->_right, tableName);
			std::string op = math2str[int(mathPtr->math_t_)];	//used for exception info

			if (left_t == right_t)
			{
				if (left_t == check_t_t::STRING && mathPtr->math_t_ != math_t_t::ADD)
				{
					throw std::string("unsupported operation '" + op + "' on string");
				}
				return left_t;
			}
			else
			{
				throw std::string("'" + op + "' on mismatched type " + check2str[int(left_t)] + ", " + check2str[int(right_t)]);
			}
		}
		case base_t_t::NUMERIC:
			return check_t_t::INT;
		case base_t_t::STR:
			return check_t_t::STRING;
		case base_t_t::ID:
		{
			const IdExpr* idPtr = static_cast<const IdExpr*>(root);
			page::col_t_t id_t;
			if (idPtr->_tableName.empty())
				id_t = table::getColumnInfo(tableName, idPtr->_columnName).col_t_;
			else
				id_t = table::getColumnInfo(idPtr->_tableName, idPtr->_columnName).col_t_;
			if (id_t == page::col_t_t::INT)
				return check_t_t::INT;
			else if (id_t == page::col_t_t::CHAR || id_t == page::col_t_t::VARCHAR)
				return check_t_t::STRING;
			//more data type...
		}

		// no expect to happen
		case base_t_t::LOGICAL_OP:
		case base_t_t::COMPARISON_OP:
		default:
			throw std::string("wrong type for atom");
		}
	}

	void checkVisit(const AtomExpr* root, const std::string tableName)
	{
#ifdef DEBUG
		if (!root)
			throw std::string("AtomExpr* root is nullptr");
#endif // DEBUG
		_checkVisit(root, tableName);
	}



	//vm visit
	int numericOp(int op1, int op2, math_t_t math_t)
	{
		switch (math_t)
		{
		case math_t_t::ADD:
			return op1 + op2;
		case math_t_t::SUB:
			return op1 - op2;
		case math_t_t::MUL:
			return op1 * op2;
		case math_t_t::DIV:
			return op1 / op2;
		case math_t_t::MOD:
			return op1 % op2;
		default:
			// no expect to happen
			throw std::string("wrong math operation type");
		}
	}

	bool comparisonOp(int op1, int op2, comparison_t_t comparison_t)
	{
		switch (comparison_t)
		{
		case comparison_t_t::EQ:
			return op1 == op2;
		case comparison_t_t::NEQ:
			return op1 != op2;
		case comparison_t_t::LESS:
			return op1 < op2;
		case comparison_t_t::GREATER:
			return op1 > op2;
		case comparison_t_t::LEQ:
			return op1 <= op2;
		case comparison_t_t::GEQ:
			return op1 >= op2;
		default:
			// no expect to happen
			throw std::string("wrong comparison operation type");
		}
	}

	bool _vmVisit(const BaseExpr* root, const table::Row* row)
	{
		base_t_t base_t = root->base_t_;
		switch (base_t)
		{
		case base_t_t::LOGICAL_OP:
		{
			const LogicalOpExpr* logicalPtr = static_cast<const LogicalOpExpr*>(root);
			RetValue left = _vmVisit(logicalPtr->_left, row);
			RetValue right = _vmVisit(logicalPtr->_right, row);
			std::string op = logical2str[int(logicalPtr->logical_t_)];  //used for exception info

			//RetValue here must be int / str
			auto op1 = std::get_if<bool>(&left);
			auto op2 = std::get_if<bool>(&right);
			if (op1 && op2)
			{	//op1 op2 are both bool
				switch (logicalPtr->logical_t_)
				{
				case logical_t_t::AND:
					return *op1 && *op2;
				case logical_t_t::OR:
					return *op1 || *op2;
				}
			}
		}
		case base_t_t::COMPARISON_OP:
		{
			const ComparisonOpExpr* comparisonPtr = static_cast<const ComparisonOpExpr*>(root);
			RetValue left = _vmVisit(comparisonPtr->_left, row);
			RetValue right = _vmVisit(comparisonPtr->_right, row);
			std::string op = comparison2str[int(comparisonPtr->comparison_t_)];

			//RetValue here must be int / str
			if (auto op1 = std::get_if<std::string>(&left))
			{	//op1 is str
				if (auto op2 = std::get_if<std::string>(&right))
				{	//op2 is also str
					switch (comparisonPtr->comparison_t_)
					{
					case comparison_t_t::EQ:
						return *op1 == *op2;
					case comparison_t_t::NEQ:
						return *op1 != *op2;
					}
				}
			}
			else if (auto op1 = std::get_if<int>(&left))
			{	//op1 is int
				if (auto op2 = std::get_if<int>(&right))
				{	//op2 is also int
					return comparisonOp(*op1, *op2, comparisonPtr->comparison_t_);
				}
			}
		}
#ifdef DEBUG
		default:
			throw std::string("WTF");
#endif // DEBUG
		}
	}

	bool vmVisit(const BaseExpr* root, const table::Row* row)
	{
#ifdef DEBUG
		if (!root)
			throw std::string("BaseExpr* root is nullptr");
#endif // DEBUG
		return _vmVisit(root, row);
	}

	RetValue _vmVisit(const AtomExpr* root, const table::Row* row)
	{
		base_t_t base_t = root->base_t_;
		switch (base_t)
		{
		case base_t_t::MATH_OP:
		{
			const MathOpExpr* mathPtr = static_cast<const MathOpExpr*>(root);
			RetValue left = _vmVisit(mathPtr->_left, row);
			RetValue right = _vmVisit(mathPtr->_right, row);
			std::string op = math2str[int(mathPtr->math_t_)];	//used for exception info

			//RetValue here must be int / str
			if (auto op1 = std::get_if<std::string>(&left))
			{	//op1 is str
				if (auto op2 = std::get_if<std::string>(&right))
				{	//op2 is also str
					if (mathPtr->math_t_ == math_t_t::ADD)
					{	//str + str
						return *op1 + *op2;
					}
				}
			}
			else if (auto op1 = std::get_if<int>(&left))
			{	//op1 is int
				if (auto op2 = std::get_if<int>(&right))
				{	//op2 is also int
					return numericOp(*op1, *op2, mathPtr->math_t_);
				}
			}
		}
		case base_t_t::NUMERIC:
		{
			const NumericExpr* numericPtr = static_cast<const NumericExpr*>(root);
			return numericPtr->_value;
		}
		case base_t_t::STR:
		{
			const StrExpr* strPtr = static_cast<const StrExpr*>(root);
			return strPtr->_value;
		}
		case base_t_t::ID:
		{
			const IdExpr* idPtr = static_cast<const IdExpr*>(root);
			if (!row)
				throw std::string("value of record cannot be used here");
			return table::vm_->getValue(row, idPtr->_tableName, idPtr->_columnName);
		}
#ifdef DEBUG
		default:
			throw std::string("WTF");
#endif // DEBUG
		}
	}

	RetValue vmVisit(const AtomExpr* root, const table::Row* row)
	{
#ifdef DEBUG
		if (!root)
			throw std::string("AtomExpr* root is nullptr");
#endif // DEBUG
		return _vmVisit(root, row);
	}
}
