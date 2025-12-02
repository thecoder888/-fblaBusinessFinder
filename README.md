# Business Finder

**FBLA Coding & Programming 2025-2026 Competition Project**  
*Byte-Sized Business Boost: Discover and support local businesses in your community*

---

## Project Overview

Business Finder is a web application designed to help users discover and support small, local businesses in their community. Built with Python Flask and integrated with the Yelp Fusion API, this application provides a comprehensive platform for finding businesses, reading reviews, saving favorites, and accessing special deals.

---

## Features

### Core FBLA Requirements 
- **Category Sorting**: Filter businesses by type (food, retail, services, beauty, fitness, arts)
- **User Reviews & Ratings**: Submit reviews with 1-5 star ratings
- **Sort by Reviews/Ratings**: Organize results by highest rated or most reviewed
- **Bookmark System**: Save favorite businesses with a simple star button
- **Special Deals & Coupons**: Admin panel for managing promotional offers
- **Bot Verification**: Google reCAPTCHA integration to prevent spam reviews

### Additional Features
- **Real-time Search**: Powered by Yelp Fusion API
- **UI/UX**: Pink and sage color scheme
- **Custom Logo**: Professionally designed Canva logo
- **Responsive Design**: Works on desktop and mobile devices
- **Database Storage**: SQLite for persistent data storage
- **Font Awesome Icons**: Professional iconography throughout

---

## Technologies Used

### Backend
- **Python 3.x**: Core programming language
- **Flask**: Web framework
- **SQLite3**: Database management
- **Requests**: HTTP library for API calls

### Frontend
- **HTML5/CSS3**: Structure and styling
- **JavaScript**: Interactive functionality
- **Font Awesome 6.4.0**: Icon library
- **Google Fonts**: Abril Fatface & Poppins typography

### APIs & Services
- **Yelp Fusion API**: Business data and search
- **Google reCAPTCHA v2**: Bot prevention

### Design
- **Custom Color Scheme**: Blush pink (#FFC1CC) to pale sage (#D4E7C5)
- **Canva**: Logo design

---

## Installation

### Prerequisites
- Python 3.7 or higher
- pip (Python package installer)
- A Yelp Fusion API key
- A Google reCAPTCHA site key and secret key

### Step 1: Clone the Repository
```bash
git clone https://github.com/thecoder888/business-finder.git
cd business-finder
```

### Step 2: Install Dependencies
```bash
pip install -r requirements.txt
```

Or install manually:
```bash
pip install flask requests python-dotenv
```

### Step 3: Set Up Environment Variables
Create a `.env` file in the root directory:
```
YELP_API_KEY=your_yelp_api_key_here
RECAPTCHA_SECRET_KEY=your_recaptcha_secret_key_here
```

**How to get API keys:**
- **Yelp API**: Sign up at https://www.yelp.com/developers
- **reCAPTCHA**: Create site at https://www.google.com/recaptcha/admin

### Step 4: Run the Application
```bash
python3 app.py
```

The application will start at `http://localhost:5000`

---

## 📁 Project Structure

```
businessFinder_project/
├── app.py                      # Main Flask application
├── .env                        # Environment variables (not tracked)
├── .gitignore                  # Git ignore file
├── requirements.txt            # Python dependencies
├── README.md                   # Project documentation
├── local_business_boost.db     # SQLite database (auto-generated)
│
├── static/
│   └── images/
│       └── logo.svg           # Custom logo
│
└── templates/
    ├── index.html             # Home page & search interface
    ├── business_detail.html   # Business details page
    └── admin.html             # Deals management admin panel
```

---

## Usage Guide

### For Users

#### 1. **Search for Businesses**
- Enter search term (e.g., "coffee", "pizza")
- Enter location (default: Des Moines, IA)
- Select category (optional)
- Choose sorting method (relevance, rating, or reviews)
- Click "Search Businesses"

#### 2. **View Business Details**
- Click on any business card from search results
- View business information, ratings, and reviews
- See contact information and Yelp link
- Check for special deals and coupons

#### 3. **Leave a Review**
- Scroll to the review form
- Enter your name
- Select star rating (1-5)
- Write review text (optional)
- **Complete reCAPTCHA verification**
- Submit review

#### 4. **Bookmark Businesses**
- Click the star icon on any business card
- Filled star = bookmarked
- Empty star = not bookmarked
- Bookmarks are saved in the database

### For Administrators

#### Access Admin Panel
Navigate to: `http://localhost:5000/admin`

#### Add a New Deal
1. Get the Business ID:
   - Search for a business
   - Click on it to view details
   - Copy the ID from the URL (e.g., `/business/abc123xyz`)

2. Fill out the form:
   - **Business ID**: Paste the copied ID
   - **Deal Title**: E.g., "20% Off All Drinks"
   - **Description**: Explain the deal details
   - **Discount Percentage**: E.g., 20
   - **Expiry Date**: Optional future date

3. Click "Add Deal"

#### View/Delete Deals
- All active deals appear below the form
- Click the trash icon to delete a deal
- Deleted deals are removed immediately

---

## Database Schema

### Tables

#### `businesses`
Cached business data from Yelp API
- `id` (TEXT, PRIMARY KEY): Yelp business ID
- `name` (TEXT): Business name
- `address`, `city`, `state`, `zip_code` (TEXT): Location details
- `category` (TEXT): Primary business category
- `yelp_rating` (REAL): Rating from Yelp
- `yelp_review_count` (INTEGER): Review count from Yelp
- `phone`, `image_url`, `yelp_url` (TEXT): Contact and reference info
- `created_at` (TIMESTAMP): Cache timestamp

#### `user_reviews`
User-submitted reviews
- `id` (INTEGER, PRIMARY KEY, AUTOINCREMENT)
- `business_id` (TEXT, FOREIGN KEY)
- `rating` (INTEGER): 1-5 stars
- `review_text` (TEXT): Review content
- `reviewer_name` (TEXT): User's name
- `created_at` (TIMESTAMP)

#### `bookmarks`
Saved favorite businesses
- `id` (INTEGER, PRIMARY KEY, AUTOINCREMENT)
- `business_id` (TEXT, FOREIGN KEY)
- `user_name` (TEXT): User identifier
- `created_at` (TIMESTAMP)
- UNIQUE constraint on (business_id, user_name)

#### `deals`
Special offers and coupons
- `id` (INTEGER, PRIMARY KEY, AUTOINCREMENT)
- `business_id` (TEXT, FOREIGN KEY)
- `title` (TEXT): Deal headline
- `description` (TEXT): Deal details
- `discount_percent` (INTEGER): Percentage off
- `expiry_date` (DATE): Deal expiration
- `created_at` (TIMESTAMP)

---

## Design Philosophy

### Color Palette
- **Primary Gradient**: Blush Pink (#FFC1CC) → Pale Sage (#D4E7C5)
- **Accent Colors**: Dusty Rose (#C5979D), Olive Green (#6B8E4E)
- **Text**: Dark Gray (#4a4a4a), Medium Gray (#666)
- **Backgrounds**: White with subtle shadows

### Typography
- **Headings**: Abril Fatface (elegant, bold)
- **Body Text**: Poppins (modern, readable)
- **Weights**: 300-700 for hierarchy

### UI Principles
- Soft rounded corners (12-20px border-radius)
- Subtle shadows for depth
- Consistent spacing and padding
- Hover effects for interactivity
- Responsive design for all screen sizes

---

## Security Features

### Bot Prevention
- Google reCAPTCHA v2 on review submissions
- Server-side verification of CAPTCHA responses
- Error handling for failed verifications

### Input Validation
- Required fields enforcement
- Rating range validation (1-5)
- Text input sanitization
- SQL injection prevention through parameterized queries

### API Security
- Environment variables for sensitive keys
- `.gitignore` to prevent key exposure
- API key validation on startup

---

## Troubleshooting

### Common Issues

**Issue**: "No module named 'flask'"
- **Solution**: Run `pip install flask requests python-dotenv`

**Issue**: "YELP_API_KEY not found"
- **Solution**: Create `.env` file with API key

**Issue**: reCAPTCHA shows "localhost not supported"
- **Solution**: Add `localhost` and `127.0.0.1` to allowed domains in Google reCAPTCHA admin

**Issue**: Templates not found
- **Solution**: Ensure templates are in `templates/` folder, not `template/`

**Issue**: Logo doesn't display
- **Solution**: Verify file is at `static/images/logo.svg`

---

## FBLA Competition Notes

### Requirements Checklist
-  Sorting businesses by category
-  Allowing users to leave reviews or ratings
-  Sorting businesses by reviews or ratings
-  Saving or bookmarking favorite businesses
-  Display special deals or coupons
-  Verification step to prevent bot activity

### Presentation Tips
1. **Demo the full user flow**: Search → View → Review → Bookmark
2. **Show the admin panel**: Add a live deal during presentation
3. **Highlight bot prevention**: Demonstrate reCAPTCHA in action
4. **Explain technology choices**: Why Flask? Why SQLite? Why Yelp API?
5. **Discuss challenges overcome**: Template folder naming, API integration, etc.

### Documentation Provided
- Comprehensive README (this file)
- Inline code comments throughout
- requirements.txt for dependencies
- .gitignore for clean repository

---

## Credits

**Developer**: Aisha Newland, Sriya Munjuluri
**School**: Waukee High School 
**FBLA Chapter**: Waukee High School Chapter
**Competition**: FBLA Coding & Programming Event 2025-2026  
**Topic**: Byte-Sized Business Boost

**APIs & Services**:
- Yelp Fusion API for business data
- Google reCAPTCHA for bot prevention
- Font Awesome for icons
- Google Fonts for typography

**Design Tools**:
- Canva for logo design
- Custom CSS for styling

---

## License

This project was created for the FBLA Coding & Programming competition. All rights reserved.

---

## Contact

For questions about this project, please contact:
- **Email**: [aishamtnewland@example.com]
- **GitHub**: [github.com/thecoder888]

---

## Acknowledgments

- **FBLA** for organizing this competition
- **Code.org** for partnering on this year's topic
- **Yelp** for providing the Fusion API
- **Google** for reCAPTCHA service

---

**Built with ❤️ for FBLA 2025-2026**