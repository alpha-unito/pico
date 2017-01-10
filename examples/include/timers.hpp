/* 
*******************************************************************************
*
* File:         timers.h
* Description:  High resolution timers on top of C++ chrono
* Author:       Maurizio Drocco
* Language:     C++
* Status:       Experimental
*
*******************************************************************************
*/


#ifndef timers_h_
#define timers_h_

#include <chrono>
using namespace std::chrono;

typedef high_resolution_clock::time_point time_point_t;
typedef duration<double> duration_t;

#define max_duration duration_t::max()
#define min_duration duration_t::min()

#define hires_timer_ull(t) (t) = high_resolution_clock::now()
#define get_duration(a,b) duration_cast<duration<double>>((b) - (a))
#define time_count(d) (d).count()

#endif
