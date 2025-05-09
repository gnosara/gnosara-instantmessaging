"""
Writing Samples Module for Gnosara

This module contains different writing samples and styles that
can be used as references for Claude when generating summaries.
"""

# Dictionary of writing styles with examples
WRITING_SAMPLES = {
    "casual": {
        "description": "Friendly, conversational tone with simple language",
        "example": """
Netflix just dropped the trailer for Full Speed season 2, and NASCAR fans are pumped! This documentary series has been bringing tons of new viewers to the sport.

While many drivers are enjoying their spring break (Ryan Blay is soaking up the sun in St. Lucia, and Denny Hamlin had the cutest father-daughter dance), Kyle Larson is gearing up for his Indy 500 challenge - he just unveiled car #17!

This weekend, don't miss the Truck Series at Rockingham and Casey Kane's racing comeback on Saturday. It's the perfect mix of NASCAR's push for new audiences and honoring its rich history.
"""
    },
    "professional": {
        "description": "More formal tone with industry terminology",
        "example": """
The release of Netflix's Full Speed season 2 trailer represents a significant milestone in NASCAR's media strategy to expand audience reach beyond traditional demographics. The documentary series has demonstrably increased viewership among segments previously untapped by conventional motorsport marketing approaches.

Concurrent with the spring hiatus, several notable competitors are utilizing the interim for personal obligations, while Kyle Larson's Indianapolis 500 preparation advances with the unveiling of his vehicle designation (#17), with Tony Kanaan confirmed as standby driver.

The weekend's competitive schedule features the Truck Series at the historic Rockingham venue and the noteworthy return of Casey Kane to competition following a multi-year absence. These developments illustrate NASCAR's dual commitment to heritage preservation and innovation in audience engagement.
"""
    },
    "enthusiastic": {
        "description": "High-energy, passionate tone with emphasis on excitement",
        "example": """
WOW! Netflix just dropped an AMAZING trailer for Full Speed season 2, and it's absolutely ELECTRIFYING! This revolutionary documentary series is bringing NASCAR to entirely new audiences in ways we've never seen before!

While our favorite drivers are enjoying some well-deserved vacation time (check out Ryan Blay living it up in gorgeous St. Lucia!), Kyle Larson is making HISTORY preparing for the incredible Indy 500/Coca-Cola 600 double - a true test of racing prowess!

You absolutely CANNOT MISS the action-packed racing at Rockingham this weekend, featuring the return of racing legend Casey Kane after years away from the track! This is NASCAR at its FINEST - honoring tradition while blazing new trails for fans everywhere!
"""
    },
    "analytical": {
        "description": "Data-focused, insightful analysis with careful examination",
        "example": """
The release of Netflix's Full Speed season 2 trailer signals the continuation of NASCAR's strategic content partnership, which data indicates has expanded audience demographics by approximately 18% in key age brackets (18-34).

During the spring competitive hiatus, driver activities demonstrate the work-life balance challenges inherent to the profession, while Kyle Larson's Indianapolis 500 preparation represents a significant cross-discipline effort with implications for both racing series' viewership overlap.

This weekend's return to Rockingham Speedway (dormant since 2013) provides a quantifiable metric of NASCAR's heritage revitalization initiative, while Casey Kane's competitive return after a 7-year absence presents an intriguing performance analysis opportunity given technological advancements during his hiatus.
"""
    },
    "minimalist": {
        "description": "Brief, concise summaries with only essential information",
        "example": """
Netflix released Full Speed season 2 trailer. Documentary brings new NASCAR fans.

Drivers on spring break: Ryan Blay (St. Lucia), Denny Hamlin (father-daughter dance).

Kyle Larson revealed Indy 500 car #17. Tony Kanaan: standby driver.

Weekend racing: Trucks at Rockingham, Xfinity features Casey Kane's return (first race since 2018).

Key impact: NASCAR expanding audience while honoring traditional venues.
"""
    }
}

# Default style to use if none specified
DEFAULT_STYLE = "casual"

def get_writing_sample(style=None):
    """
    Retrieve a writing sample based on the specified style.
    
    Args:
        style (str, optional): The writing style to retrieve. Defaults to DEFAULT_STYLE.
        
    Returns:
        dict: The writing sample with description and example
    """
    if style is None:
        style = DEFAULT_STYLE
        
    if style not in WRITING_SAMPLES:
        return WRITING_SAMPLES[DEFAULT_STYLE]
        
    return WRITING_SAMPLES[style]

def get_available_styles():
    """Return a list of all available writing styles"""
    return list(WRITING_SAMPLES.keys())

def add_writing_sample(style_name, description, example):
    """
    Add a new writing sample to the collection
    
    Args:
        style_name (str): Name of the new style
        description (str): Brief description of the style
        example (str): Example text showing the style in use
    """
    WRITING_SAMPLES[style_name] = {
        "description": description,
        "example": example
    }
