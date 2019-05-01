# Simple Video Streaming Service Application

This service is an emulation of providing video streams to customers. Videos of all the movies in the IMDB database is provided to the customers. First, creates a database for customers and specifies plans, customers, subscriptions etc. and next, it will create an application to allow customers to use the service.

Needs maven for dependencies. Fill the proper credentials in the dbconn.CONFIG file.
After running the application, you should see this in the terminal: 

*** Please enter one of the following comands ***

sign_up <email> <password> <first_name> <last_name> <plan_id>

sign_in <email> <password>

sign_out

show_plans

show_subscription

subscribe <plan_id>

watched_movie <movie_id>

search_for_movies <movie_title>

suggest_movies

quit
