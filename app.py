from flask import Flask, render_template, request, jsonify
import requests
from dotenv import load_dotenv
import os
import sqlite3
from datetime import datetime

# Load environment variables
load_dotenv()

template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=template_dir)

# Yelp API Configuration
API_KEY = os.getenv("YELP_API_KEY")
if not API_KEY:
    raise ValueError("No API key found. Please set YELP_API_KEY in your .env file.")

YELP_HEADERS = {"Authorization": f"Bearer {API_KEY}"}
YELP_URL = "https://api.yelp.com/v3/businesses/search"

# Database Configuration
DATABASE = 'local_business_boost.db'


def get_db_connection():
    """
    Create a database connection.
    
    Returns:
        sqlite3.Connection: Database connection object
    """
    conn = sqlite3.connect(DATABASE)
    conn.row_factory = sqlite3.Row  # Access columns by name
    return conn


def init_db():
    """
    Initialize the database with required tables.
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Table for cached Yelp businesses
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS businesses (
            id TEXT PRIMARY KEY,
            name TEXT NOT NULL,
            address TEXT,
            city TEXT,
            state TEXT,
            zip_code TEXT,
            category TEXT,
            yelp_rating REAL,
            yelp_review_count INTEGER,
            phone TEXT,
            image_url TEXT,
            yelp_url TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Table for user reviews
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS user_reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL,
            rating INTEGER NOT NULL CHECK(rating >= 1 AND rating <= 5),
            review_text TEXT,
            reviewer_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (business_id) REFERENCES businesses(id)
        )
    ''')
    
    # Table for bookmarked businesses
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS bookmarks (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL,
            user_name TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (business_id) REFERENCES businesses(id),
            UNIQUE(business_id, user_name)
        )
    ''')
    
    # Table for deals/coupons
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deals (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            business_id TEXT NOT NULL,
            title TEXT NOT NULL,
            description TEXT,
            discount_percent INTEGER,
            expiry_date DATE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (business_id) REFERENCES businesses(id)
        )
    ''')
    
    conn.commit()
    conn.close()


def search_yelp(term, location, category=None, limit=10):
    """
    Search for businesses on Yelp API.
    
    Args:
        term (str): Search term (e.g., "coffee", "pizza")
        location (str): Location to search in
        category (str): Optional category filter
        limit (int): Number of results to return
    
    Returns:
        list: List of business dictionaries
    """
    params = {
        "term": term,
        "location": location,
        "limit": limit
    }
    
    if category:
        params["categories"] = category
    
    try:
        response = requests.get(YELP_URL, headers=YELP_HEADERS, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        businesses = data.get("businesses", [])
        
        # Cache businesses in database
        cache_businesses(businesses)
        
        return businesses
        
    except requests.exceptions.RequestException as e:
        print(f"Yelp API Error: {e}")
        return []


def cache_businesses(businesses):
    """
    Store Yelp business data in local database.
    
    Args:
        businesses (list): List of business dictionaries from Yelp
    """
    conn = get_db_connection()
    cursor = conn.cursor()
    
    for business in businesses:
        # Extract business data
        business_id = business.get('id')
        name = business.get('name')
        location = business.get('location', {})
        address = ', '.join(location.get('display_address', []))
        city = location.get('city', '')
        state = location.get('state', '')
        zip_code = location.get('zip_code', '')
        
        # Get primary category
        categories = business.get('categories', [])
        category = categories[0].get('title') if categories else 'Other'
        
        rating = business.get('rating', 0.0)
        review_count = business.get('review_count', 0)
        phone = business.get('phone', '')
        image_url = business.get('image_url', '')
        yelp_url = business.get('url', '')
        
        # Insert or replace business data
        cursor.execute('''
            INSERT OR REPLACE INTO businesses 
            (id, name, address, city, state, zip_code, category, yelp_rating, 
             yelp_review_count, phone, image_url, yelp_url)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (business_id, name, address, city, state, zip_code, category, 
              rating, review_count, phone, image_url, yelp_url))
    
    conn.commit()
    conn.close()


def get_business_with_reviews(business_id):
    """
    Get business details with user reviews and average rating.
    
    Args:
        business_id (str): Yelp business ID
    
    Returns:
        dict: Business data with reviews
    """
    conn = get_db_connection()
    
    # Get business data
    business = conn.execute(
        'SELECT * FROM businesses WHERE id = ?', (business_id,)
    ).fetchone()
    
    if not business:
        conn.close()
        return None
    
    # Get user reviews
    reviews = conn.execute(
        'SELECT * FROM user_reviews WHERE business_id = ? ORDER BY created_at DESC',
        (business_id,)
    ).fetchall()
    
    # Get deals
    deals = conn.execute(
        'SELECT * FROM deals WHERE business_id = ?',
        (business_id,)
    ).fetchall()
    
    # Calculate combined rating
    user_ratings = [r['rating'] for r in reviews]
    if user_ratings:
        avg_user_rating = sum(user_ratings) / len(user_ratings)
        combined_rating = (business['yelp_rating'] + avg_user_rating) / 2
    else:
        combined_rating = business['yelp_rating']
    
    conn.close()
    
    return {
        'business': dict(business),
        'reviews': [dict(r) for r in reviews],
        'deals': [dict(d) for d in deals],
        'combined_rating': round(combined_rating, 1),
        'total_reviews': business['yelp_review_count'] + len(reviews)
    }


# Flask Routes

@app.route('/')
def index():
    """Home page with search form."""
    return render_template('index.html')


@app.route('/search', methods=['POST'])
def search():
    """
    Handle search requests and return results.
    """
    data = request.get_json()
    term = data.get('term', '')
    location = data.get('location', 'Des Moines, IA')
    category = data.get('category', '')
    sort_by = data.get('sort_by', 'relevance')
    
    # Search Yelp
    businesses = search_yelp(term, location, category)
    
    # Enhance with local data
    enhanced_businesses = []
    conn = get_db_connection()
    
    for business in businesses:
        business_id = business.get('id')
        
        # Get user reviews count
        user_review_count = conn.execute(
            'SELECT COUNT(*) as count FROM user_reviews WHERE business_id = ?',
            (business_id,)
        ).fetchone()['count']
        
        # Get average user rating
        avg_rating = conn.execute(
            'SELECT AVG(rating) as avg FROM user_reviews WHERE business_id = ?',
            (business_id,)
        ).fetchone()['avg']
        
        # Check if bookmarked
        is_bookmarked = conn.execute(
            'SELECT COUNT(*) as count FROM bookmarks WHERE business_id = ?',
            (business_id,)
        ).fetchone()['count'] > 0
        
        # Get deals
        deals_count = conn.execute(
            'SELECT COUNT(*) as count FROM deals WHERE business_id = ?',
            (business_id,)
        ).fetchone()['count']
        
        # Calculate combined rating
        yelp_rating = business.get('rating', 0)
        if avg_rating:
            combined_rating = (yelp_rating + avg_rating) / 2
        else:
            combined_rating = yelp_rating
        
        enhanced_businesses.append({
            **business,
            'user_review_count': user_review_count,
            'combined_rating': round(combined_rating, 1),
            'is_bookmarked': is_bookmarked,
            'has_deals': deals_count > 0
        })
    
    conn.close()
    
    # Sort results
    if sort_by == 'rating':
        enhanced_businesses.sort(key=lambda x: x['combined_rating'], reverse=True)
    elif sort_by == 'reviews':
        enhanced_businesses.sort(
            key=lambda x: x['review_count'] + x['user_review_count'], 
            reverse=True
        )
    
    return jsonify(enhanced_businesses)


@app.route('/business/<business_id>')
def business_detail(business_id):
    """
    Display detailed business information.
    """
    business_data = get_business_with_reviews(business_id)
    
    if not business_data:
        return "Business not found", 404
    
    return render_template('business_detail.html', data=business_data)


@app.route('/add_review', methods=['POST'])
def add_review():
    """
    Add a user review for a business.
    """
    data = request.get_json()
    
    # Validate input
    business_id = data.get('business_id', '').strip()
    rating = data.get('rating')
    review_text = data.get('review_text', '').strip()
    reviewer_name = data.get('reviewer_name', '').strip()
    
    # Input validation
    if not business_id or not reviewer_name:
        return jsonify({'error': 'Business ID and reviewer name are required'}), 400
    
    if not rating or not (1 <= int(rating) <= 5):
        return jsonify({'error': 'Rating must be between 1 and 5'}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('''
            INSERT INTO user_reviews (business_id, rating, review_text, reviewer_name)
            VALUES (?, ?, ?, ?)
        ''', (business_id, int(rating), review_text, reviewer_name))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Review added successfully'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/toggle_bookmark', methods=['POST'])
def toggle_bookmark():
    """
    Add or remove a business bookmark.
    """
    data = request.get_json()
    business_id = data.get('business_id')
    user_name = data.get('user_name', 'default_user')
    
    if not business_id:
        return jsonify({'error': 'Business ID required'}), 400
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Check if already bookmarked
    existing = cursor.execute(
        'SELECT id FROM bookmarks WHERE business_id = ? AND user_name = ?',
        (business_id, user_name)
    ).fetchone()
    
    if existing:
        # Remove bookmark
        cursor.execute(
            'DELETE FROM bookmarks WHERE business_id = ? AND user_name = ?',
            (business_id, user_name)
        )
        bookmarked = False
    else:
        # Add bookmark
        cursor.execute(
            'INSERT INTO bookmarks (business_id, user_name) VALUES (?, ?)',
            (business_id, user_name)
        )
        bookmarked = True
    
    conn.commit()
    conn.close()
    
    return jsonify({'success': True, 'bookmarked': bookmarked})


if __name__ == '__main__':
    # Initialize database on startup
    init_db()
    
    # Run Flask app
    app.run(debug=True, port=5000)     