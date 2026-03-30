from flask import Flask, render_template, request, jsonify
import requests
from dotenv import load_dotenv
import os
import sqlite3
from datetime import datetime

# Load environment variables
load_dotenv()

# Get the absolute path to the templates folder
template_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'templates')
app = Flask(__name__, template_folder=template_dir)

# Yelp API Configuration
API_KEY = os.getenv("YELP_API_KEY")
if not API_KEY:
    raise ValueError("No API key found. Please set YELP_API_KEY in your .env file.")

YELP_HEADERS = {"Authorization": f"Bearer {API_KEY}"}
YELP_URL = "https://api.yelp.com/v3/businesses/search"

# reCAPTCHA Configuration
RECAPTCHA_SECRET_KEY = os.getenv("RECAPTCHA_SECRET_KEY")
RECAPTCHA_VERIFY_URL = "https://www.google.com/recaptcha/api/siteverify"

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


def verify_recaptcha(recaptcha_response):
    """
    Verify reCAPTCHA response with Google.
    
    Args:
        recaptcha_response (str): Response token from client
    
    Returns:
        bool: True if verification successful, False otherwise
    """
    if not RECAPTCHA_SECRET_KEY:
        print("Warning: RECAPTCHA_SECRET_KEY not set")
        return True  # Allow in development if key not set
    
    try:
        response = requests.post(RECAPTCHA_VERIFY_URL, data={
            'secret': RECAPTCHA_SECRET_KEY,
            'response': recaptcha_response
        }, timeout=5)
        
        result = response.json()
        return result.get('success', False)
    except Exception as e:
        print(f"reCAPTCHA verification error: {e}")
        return False


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
    Add a user review for a business with comprehensive validation.
    Validates on both syntactical and semantic levels.
    """
    data = request.get_json()
    
    # Verify reCAPTCHA (bot prevention)
    recaptcha_response = data.get('recaptcha_response', '')
    if not verify_recaptcha(recaptcha_response):
        return jsonify({'error': 'reCAPTCHA verification failed. Please try again.'}), 400
    
    # Get input data
    business_id = data.get('business_id', '').strip()
    rating = data.get('rating')
    review_text = data.get('review_text', '').strip()
    reviewer_name = data.get('reviewer_name', '').strip()
    
    # SYNTACTICAL VALIDATION (format and data type checks)
    if not business_id or not reviewer_name:
        return jsonify({'error': 'Business ID and reviewer name are required'}), 400
    
    if not rating or not (1 <= int(rating) <= 5):
        return jsonify({'error': 'Rating must be between 1 and 5'}), 400
    
    # Validate name format and length
    if len(reviewer_name) < 2 or len(reviewer_name) > 50:
        return jsonify({'error': 'Name must be between 2 and 50 characters'}), 400
    
    if any(char.isdigit() for char in reviewer_name):
        return jsonify({'error': 'Name cannot contain numbers'}), 400
    
    # Validate review text length if provided
    if review_text and len(review_text) > 500:
        return jsonify({'error': 'Review must be under 500 characters'}), 400
    
    # SEMANTIC VALIDATION (meaning and business logic checks)
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if business exists in database
        business_exists = cursor.execute(
            'SELECT id FROM businesses WHERE id = ?', (business_id,)
        ).fetchone()
        
        if not business_exists:
            conn.close()
            return jsonify({'error': 'Business not found. Please search for the business first.'}), 400
        
        # Insert review into database
        cursor.execute('''
            INSERT INTO user_reviews (business_id, rating, review_text, reviewer_name)
            VALUES (?, ?, ?, ?)
        ''', (business_id, int(rating), review_text, reviewer_name))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Review added successfully'})
        
    except Exception as e:
        return jsonify({'error': f'Database error: {str(e)}'}), 500


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


# Admin Routes for Deals Management

@app.route('/admin')
def admin():
    """
    Admin page for managing deals.
    """
    return render_template('admin.html')


@app.route('/help')
def help_page():
    """
    Help and FAQ page for users.
    """
    return render_template('help.html')


@app.route('/admin/add_deal', methods=['POST'])
def add_deal():
    """
    Add a new deal for a business with validation.
    """
    data = request.get_json()
    
    business_id = data.get('business_id', '').strip()
    title = data.get('title', '').strip()
    description = data.get('description', '').strip()
    discount_percent = data.get('discount_percent')
    expiry_date = data.get('expiry_date')
    
    # SYNTACTICAL VALIDATION
    if not business_id or not title:
        return jsonify({'error': 'Business ID and title are required'}), 400
    
    if len(title) < 3 or len(title) > 100:
        return jsonify({'error': 'Title must be between 3 and 100 characters'}), 400
    
    if description and len(description) > 300:
        return jsonify({'error': 'Description must be under 300 characters'}), 400
    
    if discount_percent:
        try:
            discount = int(discount_percent)
            if discount < 0 or discount > 100:
                return jsonify({'error': 'Discount must be between 0 and 100'}), 400
        except ValueError:
            return jsonify({'error': 'Discount must be a valid number'}), 400
    
    # SEMANTIC VALIDATION
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Check if business exists
        business_exists = cursor.execute(
            'SELECT id FROM businesses WHERE id = ?', (business_id,)
        ).fetchone()
        
        if not business_exists:
            conn.close()
            return jsonify({'error': 'Business ID not found. Please search for the business first.'}), 400
        
        # Insert deal
        cursor.execute('''
            INSERT INTO deals (business_id, title, description, discount_percent, expiry_date)
            VALUES (?, ?, ?, ?, ?)
        ''', (business_id, title, description, discount_percent, expiry_date))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Deal added successfully'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/admin/get_deals')
def get_deals():
    """
    Get all deals with business information.
    """
    conn = get_db_connection()
    
    deals = conn.execute('''
        SELECT d.*, b.name as business_name
        FROM deals d
        LEFT JOIN businesses b ON d.business_id = b.id
        ORDER BY d.created_at DESC
    ''').fetchall()
    
    conn.close()
    
    return jsonify([dict(deal) for deal in deals])


@app.route('/admin/delete_deal', methods=['POST'])
def delete_deal():
    """
    Delete a deal.
    """
    data = request.get_json()
    deal_id = data.get('deal_id')
    
    if not deal_id:
        return jsonify({'error': 'Deal ID required'}), 400
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute('DELETE FROM deals WHERE id = ?', (deal_id,))
        
        conn.commit()
        conn.close()
        
        return jsonify({'success': True, 'message': 'Deal deleted successfully'})
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    # Initialize database on startup
    init_db()
    
    # Run Flask app
    app.run(debug=True, port=5000)