#include "include/lexer.h"
#include <functional>
#include <unordered_set>

using namespace DB::lexer;
using std::unordered_set;
using std::size_t;
using std::function;
using std::vector;
using std::string;
using std::variant;
using std::make_tuple;
using std::unordered_map;
using keyword_it = unordered_map<string, type>::const_iterator;

namespace DB::util
{
	template<class... Ts> struct overloaded : Ts... { using Ts::operator()...; };
	template<class... Ts> overloaded(Ts...)->overloaded<Ts...>;
}

namespace DB::lexer {
	std::string type2str(type _type) noexcept
	{
		auto it = keyword2str.find(_type);
		if (it == keyword2str.end())return "No such type";
		return it->second;
	}
	type num_t2type(numeric_type num_t) noexcept {
		return static_cast<type>(static_cast<std::size_t>(type::INT)
			+ static_cast<std::size_t>(num_t) - static_cast<std::size_t>(numeric_type::INT));
	}
}

/**
 * analyzers
 */
namespace DB::lexer::analyzers {
	using analyzer = function<bool(const char *, size_t&, const size_t, vector<token_info> &)>;

	const static unordered_map<char, char> escapingMap = {
			{ 't', '\t' }, { 'n', '\n' }, { 'r', '\r' }, { 'a', '\a' }, { 'b', '\b' }, { 'f', '\f' }
	};
	const static unordered_set<char> combinableOperatorSet = {
			'!', '=', '<', '>', //'%', '.', ':', '+', '^', '*', '/',
	};
	const static unordered_set<char> singleOperatorSet = {
			'(', ')', '{', '}', '[', ']', ',', '?', '%', '.', ':', '+', '^', '*', '/', '$',
	};
	const static unordered_set<char> secondCombinableOperatorSet = {
			'=', //'|', '&', '.', ':', '<', '>', '+',
	};
	const static unordered_set<char> dividerCharSet = {
			' ', '\n', '\t'
	};

	inline bool isDivider(char c) {
		return dividerCharSet.find(c) != dividerCharSet.end();
	}
	inline bool isWordBeginning(char c) {
		return (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_';
	}
	inline bool isNum(char c) {
		return c >= '0' && c <= '9';
	}
	inline bool isNumBegin(char c) {
		return isNum(c) || c == '.';
	}
	inline bool canInWord(char c) {
		return isWordBeginning(c) || isNum(c);
	}
	inline bool isCombinableOperatorChar(char c) {
		return combinableOperatorSet.find(c) != combinableOperatorSet.end();
	}
	inline bool isSingleOperatorChar(char c) {
		return singleOperatorSet.find(c) != singleOperatorSet.end();
	}
	inline bool isSecondCombinableOpearatorChar(char c) {
		return secondCombinableOperatorSet.find(c) != secondCombinableOperatorSet.end();
	}
	

#define analyzer_type(funcname) bool funcname(const char *s, size_t& pos, const size_t size, vector<token_info> &r)

	analyzer_type(word_analyzer) {
		if (!isWordBeginning(s[pos]))
			return false;
		size_t length = 1;
		for (; canInWord(s[pos + length]); length++);

		string ns(s + pos, length);
		keyword_it it = keywords.find(ns);
		if (it != keywords.end())
			r.push_back(make_tuple((it->second), pos));
		else
			r.push_back(make_tuple((std::move(ns)), pos));

		pos += length;
		return true;
	}

	// get the longest operators
	analyzer_type(combinable_operator_analyzer) {
		if (!isCombinableOperatorChar(s[pos]))
			return false;

		size_t begin = pos;
		while (isSecondCombinableOpearatorChar(s[++pos]));

		keyword_it it;
		for (; (it = keywords.find(string(s + begin, pos - begin))) == keywords.end(); --pos);
		r.push_back(make_tuple((it->second), pos));

		return true;
	}

	analyzer_type(single_operator_analyzer) {
		if (!isSingleOperatorChar(s[pos]))
			return false;

		r.push_back(make_tuple((keywords.find(string(1, s[pos++]))->second), pos));
		return true;
	}

	// returns the char value in int
	int escapeTackle(const char *s, size_t& pos, const size_t size, vector<token_info> &r) {
		//here pos is in '\'
		int value = 0;

		char c = s[++pos];
		unordered_map<char, char>::const_iterator p = escapingMap.find(c);
		if (p != escapingMap.end())
			value = p->second;
		else // like \', \", \\ that's just the same with '\c' which means 'c' itself
			value = c;

		return value;
	}

	analyzer_type(string_analyzer) {
		if (s[pos] != '"' && s[pos] !='\'')
			return false;

		char endChar = s[pos];
		string ns;

		for (++pos; pos < size && s[pos] != endChar; )
			if (s[pos] == '\\')
				ns += (char)escapeTackle(s, pos, size, r); // it will point to next char to check
			else
				ns += s[pos++]; // also next char to check

		if (pos == size)
			throw Token_Ex("expected a corresponding " + string(1, endChar) + " to end string", pos);

		r.push_back(make_tuple((make_tuple(std::move(ns))), pos));

		++pos;
		return true;
	}

	//need to deal with oversize 
	analyzer_type(number_analyzer) {
		if (!isNum(s[pos]) )
			return false;
		int value = 0;
		numeric_type type = numeric_type::INT;

		while (pos < size) {
			char c = s[pos];
			if (isNum(c))
				value = value * 10 + (c - '0');
			else if (c != '_')
				break;
			++pos;
		}

		r.push_back(make_tuple((std::tuple<const int, const numeric_type>(value, type)), pos));

		return true;
	}

#undef write_analyzer
}

constexpr int analyzerNum = 5; 
analyzers::analyzer analyzer[] = {
		analyzers::word_analyzer,
		analyzers::number_analyzer,
		analyzers::single_operator_analyzer,
		analyzers::combinable_operator_analyzer,
		analyzers::string_analyzer,
};


namespace DB::lexer
{
	std::variant<std::vector<token_info>, analyzers::Token_Ex> tokenize(const char *s, const size_t size) noexcept {
		vector<token_info> r;
		bool ok;
		for (size_t pos = 0; pos < size; ) {
			ok = false;
			if (analyzers::isDivider(s[pos])) {
				++pos;
				continue;
			}
			for (size_t i = 0; i < analyzerNum; i++) {
				//if it returns true, means it've done, and there's no need to tackle this round again
				try {
					ok = analyzer[i](s, pos, size, r);
					if (ok)
						break;
				}
				catch (analyzers::Token_Ex& e) {
					return analyzers::Token_Ex(
						e._msg + "\"" + s[e._position] + "\"", e._position
					);
				}
			}
			if (!ok)
				return analyzers::Token_Ex(
					std::string("not a recognizable character.") + "\"" + s[pos] + "\"", pos
				);
		}

		return r;
	} // end fuction tokenize();


	/*
	 * class member function for Lexer.
	 */
	void Lexer::tokenize(const char *s, const size_t size)
	{
		_token_stream.clear();
		auto result = DB::lexer::tokenize(s, size);
		std::visit(util::overloaded{
			[](const DB::lexer::analyzers::Token_Ex& e) {
				throw DB::DB_Universal_Exception{
						std::move(const_cast<DB::lexer::analyzers::Token_Ex&>(e)._msg),
						e._position };
			},
			[this](const std::vector<DB::lexer::token_info>& tokens) {
				for (token_info const& token : tokens)
					this->_token_stream.push_back(token);
			},
			}, result);
	}

	std::size_t Lexer::size() const { return _token_stream.size(); }

	bool Lexer::empty() const { return _token_stream.empty(); }

#define CHECK_EMPTY_AND_UPDATE		\
	do{                                                  \
		if (_token_stream.empty())                                                      \
			throw DB_Universal_Exception("no more tokens", cur_pos);     \
		cur_pos = _token_stream.front()._pos;                                           \
	} while(0)

	Token Lexer::getToken() {
		CHECK_EMPTY_AND_UPDATE;
		return _token_stream.front();
	}

	void Lexer::popToken() {
		CHECK_EMPTY_AND_UPDATE;
		_token_stream.pop_front();
	}

	Token Lexer::consumeToken() {
		CHECK_EMPTY_AND_UPDATE;
		const Token token = _token_stream.front();
		_token_stream.pop_front();
		return token;
	}


	void Lexer::print(std::ostream& out) const {
		for (Token const& token : _token_stream)
		{
			out <<  "pos:" << token._pos << "\t\t";
			std::visit(util::overloaded{
					[&out](const DB::lexer::type& _type) { out << "type: " << "\t\t\t" << std::quoted(DB::lexer::type2str(_type)) << std::endl; },
					[&out](const DB::lexer::identifier& _identifier) { out << "identifier: " << "\t\t" << _identifier << std::endl; },
					[&out](const DB::lexer::numeric_t& _num) { out << "numeric: " << "\t\t" << std::quoted(DB::lexer::type2str(num_t2type(std::get<const DB::lexer::numeric_type>(_num)))) << "\t"; out << std::get<const int>(_num); out << std::endl; },
					[&out](const DB::lexer::string_literal_t& _str) { out << "string literal: " << "\t" << std::quoted(std::get<const std::string>(_str)) << std::endl; },
					[&out](auto) { out << "unexpected tokenizer" << std::endl; },
				}, token._token);
		}
	}

	std::deque<Token> Lexer::getTokens()
	{
		return this->_token_stream;
	}

	size_t getType(const Token& token)
	{
		type t = std::visit(DB::util::overloaded{
				[](const lexer::type& type) { return type; },
				[](const lexer::identifier) { return lexer::type::IDENTIFIER; },
				[](const lexer::numeric_t) { return lexer::type::NUMBER_CONSTANT; },
				[](const lexer::string_literal_t) { return lexer::type::STR_LITERAL; },
			}, token._token);
		return size_t(t);
	}

} // end namespace DB::lexer
