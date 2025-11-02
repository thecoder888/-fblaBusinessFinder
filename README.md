# Byte-Sized Business Boost 🏪

FBLA Coding & Programming 2025-2026 Competition Project

## Overview
A web application that helps users discover and support small, local businesses in their community using the Yelp Fusion API.

## Features
- 🔍 Search businesses by category and location
- ⭐ User reviews and ratings system
- 📌 Bookmark favorite businesses
- 🎉 Special deals and coupons display
- 📊 Sort by ratings or review count
- 💾 SQLite database for local data storage

## Technologies Used
- **Backend:** Python, Flask
- **Database:** SQLite3
- **API:** Yelp Fusion API
- **Frontend:** HTML, CSS, JavaScript

## Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/fbla-business-finder.git
cd fbla-business-finder
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Create a `.env` file and add your Yelp API key:
```
YELP_API_KEY=your_api_key_here
```

4. Run the application:
```bash
python yelp_test.py
```

5. Open your browser to `http://localhost:5000`

## Project Structure
```
businessFinder_project/
├── yelp_test.py              # Main Flask application
├── templates/
│   ├── index.html            # Home page
│   └── business_detail.html  # Business details page
├── requirements.txt          # Python dependencies
└── .env                      # API keys (not tracked)
```

## FBLA Competition Requirements Met
- ✅ Sorting businesses by category
- ✅ User reviews and ratings
- ✅ Sorting by reviews/ratings
- ✅ Bookmarking favorite businesses
- ✅ Special deals display
- ⚠️ Bot verification (in progress)

## Author
Aisha Newland - Waukee] - FBLA Chapter