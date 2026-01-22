"""
Culture Analysis Module
Implements Hofstede and MIT Big 9 culture dimension scoring
"""

import re
from typing import Dict, Optional, Tuple
import json

# Hofstede Dimension Dictionaries
HOFSTEDE_DIMENSIONS = {
    "process_results": {
        "process_oriented": [
            "procedures", "compliance", "following rules", "bureaucratic", "red tape",
            "step-by-step", "documentation", "sign-offs", "approvals required",
            "by the book", "sops", "protocols", "checklists", "audits",
            "risk-averse", "cautious", "methodical", "structured process",
            "process-driven", "process-focused", "procedural", "formal process"
        ],
        "results_oriented": [
            "outcomes", "targets", "goals", "performance-driven", "results matter",
            "delivery", "impact", "achieving objectives", "bottom line",
            "move fast", "bias for action", "get things done", "entrepreneurial",
            "accountability", "ownership", "make it happen", "results speak",
            "results-oriented", "outcome-focused", "performance metrics", "delivering results"
        ]
    },
    "job_employee": {
        "job_oriented": [
            "task completion", "task-focused", "job-focused", "work-focused",
            "efficiency", "productivity", "output", "deliverables",
            "get the job done", "focus on work", "work first", "task oriented"
        ],
        "employee_oriented": [
            "employee wellbeing", "work-life balance", "employee satisfaction",
            "employee development", "employee growth", "caring", "supportive",
            "family-friendly", "personal development", "employee first",
            "wellbeing", "people-focused", "employee-centric", "human-centered"
        ]
    },
    "professional_parochial": {
        "professional": [
            "professional", "industry expertise", "technical skills", "specialist",
            "professional development", "career advancement", "industry knowledge",
            "professional standards", "professional ethics", "expert"
        ],
        "parochial": [
            "company culture", "company loyalty", "company values", "company first",
            "company-focused", "internal culture", "company identity",
            "company commitment", "loyalty to company", "insider"
        ]
    },
    "open_closed": {
        "open_system": [
            "diverse perspectives", "external hires", "new ideas welcome",
            "learning culture", "bring in talent", "outside thinking",
            "collaborative", "cross-functional", "inclusive", "meritocratic",
            "challenge status quo", "fresh perspectives", "innovative",
            "open to change", "welcoming", "open-minded", "receptive"
        ],
        "closed_system": [
            "insider culture", "old boys network", "tenure matters",
            "hard to break in", "cliquey", "politics", "who you know",
            "not invented here", "resistant to change", "traditional",
            "established ways", "that's how we do it", "outsiders struggle",
            "insular", "closed-minded", "resistant", "exclusive"
        ]
    },
    "tight_loose": {
        "tight_control": [
            "hierarchy", "chain of command", "formal", "structured",
            "micromanagement", "approvals", "sign-off culture", "bureaucracy",
            "rules", "policies", "standardised", "compliance-focused",
            "top-down", "command and control", "rigid", "strict",
            "hierarchical", "formal structure", "control", "oversight"
        ],
        "loose_control": [
            "autonomy", "empowerment", "trust", "self-directed",
            "flat structure", "flexible", "agile", "startup culture",
            "freedom", "independent", "ownership", "entrepreneurial",
            "minimal bureaucracy", "fast decisions", "bottom-up", "decentralised",
            "autonomous", "flexible", "empowered", "self-managing"
        ]
    },
    "pragmatic_normative": {
        "pragmatic": [
            "results-over-rules", "pragmatic", "practical", "market-driven",
            "customer-focused", "flexible", "adaptable", "realistic",
            "business-focused", "profit-driven", "bottom line", "efficiency"
        ],
        "normative": [
            "values-driven", "principle-adherence", "ethical", "integrity",
            "mission-driven", "purpose-driven", "values-based", "principled",
            "moral", "ethical standards", "doing the right thing", "values matter"
        ]
    }
}

# MIT Big 9 Dimension Keywords
MIT_BIG_9_KEYWORDS = {
    "agility": [
        "agile", "fast", "quick", "responsive", "adaptable", "flexible",
        "speed", "nimble", "rapid", "swift", "quick to adapt", "quick response",
        "ability to change", "quick decision", "fast moving", "dynamic"
    ],
    "collaboration": [
        "collaborative", "teamwork", "team", "cooperation", "cross-functional",
        "working together", "team player", "collaborative culture", "cooperation",
        "communicate", "together", "partnership", "collective", "unified"
    ],
    "customer_orientation": [
        "customer", "client", "customer-focused", "customer-centric", "customer first",
        "customer satisfaction", "customer needs", "customer service", "customer-oriented",
        "focus on customers", "customer-driven", "customer experience"
    ],
    "diversity": [
        "diverse", "diversity", "inclusive", "inclusion", "different backgrounds",
        "varied", "multicultural", "equal opportunity", "representation",
        "different perspectives", "inclusive environment", "welcoming"
    ],
    "execution": [
        "execution", "deliver", "delivery", "execute", "get things done",
        "deliver on commitments", "follow through", "accountability",
        "execution excellence", "deliver results", "execution focused"
    ],
    "innovation": [
        "innovation", "innovative", "creative", "creativity", "new ideas",
        "new products", "experimental", "forward-thinking", "cutting-edge",
        "think outside the box", "breakthrough", "pioneering", "novel"
    ],
    "integrity": [
        "integrity", "ethical", "honest", "honesty", "trustworthy", "trust",
        "transparent", "transparency", "authentic", "authentic culture",
        "doing the right thing", "ethical behavior", "moral"
    ],
    "performance": [
        "performance", "meritocracy", "merit-based", "high performers",
        "performance-driven", "accountability", "results-oriented",
        "performance culture", "high standards", "excellence", "high bar"
    ],
    "respect": [
        "respect", "respectful", "dignity", "consideration", "valued",
        "psychological safety", "safe", "supportive", "caring",
        "treat people well", "respect for people", "human dignity"
    ]
}

def score_review_with_dictionary(review_text: str) -> Dict:
    """
    Score a review using dictionary-based approach.
    Returns scores for all Hofstede and MIT Big 9 dimensions.
    """
    if not review_text or not isinstance(review_text, str):
        return None
    
    text_lower = review_text.lower()
    scores = {
        "hofstede": {},
        "mit_big_9": {},
        "scoring_method": "dictionary"
    }
    
    # Score Hofstede dimensions
    for dimension, poles in HOFSTEDE_DIMENSIONS.items():
        pole_a_key = list(poles.keys())[0]  # First key (negative pole)
        pole_b_key = list(poles.keys())[1]  # Second key (positive pole)
        
        pole_a_count = sum(1 for phrase in poles[pole_a_key] if phrase in text_lower)
        pole_b_count = sum(1 for phrase in poles[pole_b_key] if phrase in text_lower)
        total_evidence = pole_a_count + pole_b_count
        
        if total_evidence == 0:
            score = None  # Not discussed
        else:
            # Score from -1 to +1
            score = (pole_b_count - pole_a_count) / total_evidence
        
        scores["hofstede"][dimension] = {
            "score": score,
            "confidence": "medium" if score is not None else "low",
            "evidence_count": total_evidence
        }
    
    # Score MIT Big 9 dimensions (0-10 scale)
    for dimension, keywords in MIT_BIG_9_KEYWORDS.items():
        count = sum(1 for keyword in keywords if keyword in text_lower)
        
        # Convert count to 0-10 scale (max 10 keywords = 10 points)
        score = min(10, count * 2) if count > 0 else 0
        
        scores["mit_big_9"][dimension] = {
            "score": score,
            "confidence": "medium" if score > 0 else "low",
            "evidence_count": count
        }
    
    return scores

def aggregate_review_scores(review_scores_list: list) -> Dict:
    """
    Aggregate individual review scores to company-level profile.
    Calculates mean and standard deviation for each dimension.
    """
    if not review_scores_list or len(review_scores_list) == 0:
        return None
    
    import statistics
    
    aggregated = {
        "hofstede": {},
        "mit_big_9": {},
        "review_count": len(review_scores_list)
    }
    
    # Aggregate Hofstede dimensions
    hofstede_dimensions = [
        "process_results", "job_employee", "professional_parochial",
        "open_closed", "tight_loose", "pragmatic_normative"
    ]
    
    for dimension in hofstede_dimensions:
        scores = [
            r["hofstede"][dimension]["score"]
            for r in review_scores_list
            if r and r.get("hofstede", {}).get(dimension, {}).get("score") is not None
        ]
        evidence_counts = [
            r["hofstede"][dimension].get("evidence_count", 0)
            for r in review_scores_list
            if r and r.get("hofstede", {}).get(dimension, {}).get("score") is not None
        ]
        
        if scores:
            aggregated["hofstede"][dimension] = {
                "mean": statistics.mean(scores),
                "std": statistics.stdev(scores) if len(scores) > 1 else 0,
                "count": len(scores),
                "total_evidence": sum(evidence_counts)
            }
    
    # Aggregate MIT Big 9 dimensions
    mit_dimensions = [
        "agility", "collaboration", "customer_orientation", "diversity",
        "execution", "innovation", "integrity", "performance", "respect"
    ]
    
    for dimension in mit_dimensions:
        scores = [
            r["mit_big_9"][dimension]["score"]
            for r in review_scores_list
            if r and r.get("mit_big_9", {}).get(dimension, {}).get("score") is not None
        ]
        evidence_counts = [
            r["mit_big_9"][dimension].get("evidence_count", 0)
            for r in review_scores_list
            if r and r.get("mit_big_9", {}).get(dimension, {}).get("score") is not None
        ]
        
        if scores:
            aggregated["mit_big_9"][dimension] = {
                "mean": statistics.mean(scores),
                "std": statistics.stdev(scores) if len(scores) > 1 else 0,
                "count": len(scores),
                "total_evidence": sum(evidence_counts)
            }
    
    return aggregated

def score_review_with_claude(review_text: str, client) -> Optional[Dict]:
    """
    Score a review using Claude API for higher accuracy.
    Falls back to dictionary if API fails.
    """
    try:
        prompt = f"""Analyze this employee review and score the company's culture on each dimension 
from -1 to +1. Use the following scale:
-1 = Strongly exhibits Pole A
-0.5 = Moderately exhibits Pole A
0 = Neutral or not discussed
+0.5 = Moderately exhibits Pole B
+1 = Strongly exhibits Pole B

Hofstede Dimensions:
1. Process vs Results (-1 = process-focused, +1 = results-focused)
2. Job vs Employee (-1 = task-focused, +1 = employee wellbeing focused)
3. Professional vs Parochial (-1 = profession-focused, +1 = company-focused)
4. Open vs Closed (-1 = closed/insular, +1 = open to new ideas)
5. Tight vs Loose Control (-1 = hierarchical/controlled, +1 = autonomous/flexible)
6. Pragmatic vs Normative (-1 = results-over-rules, +1 = values-driven)

MIT Big 9 Dimensions (0-10 scale):
- Agility (speed and flexibility)
- Collaboration (teamwork)
- Customer Orientation (focus on customers)
- Diversity (inclusive environment)
- Execution (deliver on commitments)
- Innovation (creativity and new ideas)
- Integrity (ethical behavior)
- Performance (meritocracy)
- Respect (dignity and consideration)

Review: "{review_text[:2000]}"

Respond in JSON format with:
{{
    "hofstede": {{
        "process_results": {{"score": 0.5, "confidence": "high"}},
        "job_employee": {{"score": -0.2, "confidence": "medium"}},
        "professional_parochial": {{"score": 0.1, "confidence": "low"}},
        "open_closed": {{"score": 0.7, "confidence": "high"}},
        "tight_loose": {{"score": -0.3, "confidence": "medium"}},
        "pragmatic_normative": {{"score": 0.4, "confidence": "high"}}
    }},
    "mit_big_9": {{
        "agility": {{"score": 7, "confidence": "high"}},
        "collaboration": {{"score": 6, "confidence": "medium"}},
        "customer_orientation": {{"score": 5, "confidence": "medium"}},
        "diversity": {{"score": 4, "confidence": "low"}},
        "execution": {{"score": 8, "confidence": "high"}},
        "innovation": {{"score": 6, "confidence": "medium"}},
        "integrity": {{"score": 7, "confidence": "high"}},
        "performance": {{"score": 7, "confidence": "high"}},
        "respect": {{"score": 6, "confidence": "medium"}}
    }}
}}"""
        
        message = client.messages.create(
            model="claude-3-5-sonnet-20241022",
            max_tokens=1024,
            messages=[
                {"role": "user", "content": prompt}
            ]
        )
        
        response_text = message.content[0].text
        # Extract JSON from response
        json_start = response_text.find('{')
        json_end = response_text.rfind('}') + 1
        if json_start >= 0 and json_end > json_start:
            json_str = response_text[json_start:json_end]
            scores = json.loads(json_str)
            scores["scoring_method"] = "llm"
            return scores
        else:
            return score_review_with_dictionary(review_text)
    
    except Exception as e:
        print(f"Claude API error: {e}, falling back to dictionary")
        return score_review_with_dictionary(review_text)
