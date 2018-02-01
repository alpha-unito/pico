/*
 * Key_generator.hpp
 *
 *  Created on: Jan 28, 2018
 *      Author: martinelli
 */


/*
 * return the keys passed in input respecting the order.
 */

class Key_generator{
public:
	Key_generator(std::initializer_list<std::string> init_list) : keys{init_list}, i{0}{

	}
	//return the next key
	std::string next_key(){
		std::string key;
		assert(i < keys.size());
		key = keys[i];
		++i;
		return key;
	}
private:
	std::vector<std::string> keys;
	std::size_t i;
};
