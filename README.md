Mortgage Analytics Modules

This repository contains various modules designed to streamline and optimize different aspects of mortgage analytics. Each module is crafted to handle specific tasks related to data extraction, processing, and analysis.
Modules:
Housing Index

    Description: Extracts, visualizes, and analyzes data from the Zillow Housing Index dataset, specifically tailored for single-family homes.
    Technologies: Python
    Features:
        Data extraction from Zillow Housing Index
        Visualization and analysis of housing data

CompileDBQueries

    Description: Combines, cleans, and optimizes database queries from QED's database. Designed to handle large ranges of data, with each query potentially taking from a few minutes to overnight to complete.
    Technologies: Python, SQL
    Features:
        Rebuilds multiple datasets into a single dataframe
        Performs operations such as rate correction, date cleaning, and address lookup

ParsePaperShuffle

    Description: Scans and processes paperwork before printing and sending to clients. Removes redundant pages and extracts pages requiring signatures, significantly reducing office overhead.
    Technologies: Python
    Features:
        Reduces redundant paperwork
        Extracts pages needing signatures

scrapeFAR

    Description: Automatically pulls the weekly ratesheet from Finance of America for reverse mortgages. Utilizes computer vision and PDF parsing to extract relevant data.
    Technologies: Python, Minecart (PDF parsing)
    Features:
        Automates ratesheet extraction
        Integrates data into internal systems

scrapeMarket

    Description: Pulls financial data from NASDAQ's API and the Wall Street Journal's LIBOR webpage. This data informs market movements and rate changes in the internal database.
    Technologies: Python, API Integration
    Features:
        Extracts financial data from NASDAQ and Wall Street Journal
        Feeds data into internal databases

validateAddress

    Description: A standalone address validation module that uses USPS's API to update address data and Zillow URLs in the database. Scrapes current valuations from Zillow.
    Note: This module is a proof of concept and not used in production due to Zillow's Terms of Service.
    Technologies: Python, USPS API
    Features:
        Validates and updates address data
        Scrapes Zillow for current valuations (proof of concept)

