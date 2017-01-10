/* 
 *******************************************************************************
 *
 * File:         utils.hpp
 * Description:  sparse utilities
 * Author:       Maurizio Drocco
 * Language:     C++
 * Status:       Experimental
 * Created on:   Jan 10, 2017
 *
 *******************************************************************************
 */
#ifndef EXAMPLES_INCLUDE_UTILS_HPP_
#define EXAMPLES_INCLUDE_UTILS_HPP_

/* some verbose printing */
void print_progress(float progress)
{
    int barWidth = 70;
    int pos = barWidth * progress;

    std::cerr << "[";
    for (int i = 0; i < barWidth; ++i)
    {
        if (i < pos)
            std::cerr << "=";
        else if (i == pos)
            std::cerr << ">";
        else
            std::cerr << " ";
    }
    std::cerr << "] " << int(progress * 100.0) << " %\r";
    std::cerr.flush();
}



#endif /* EXAMPLES_INCLUDE_UTILS_HPP_ */
