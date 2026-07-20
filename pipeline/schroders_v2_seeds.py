"""v2 bipolar dimension definitions (b01-b12) and seed phrases per pole.

Seeds are the semantic anchors for automated dictionary expansion (Phase 2).
They are phrases observed in Glassdoor review language, 8-12 per pole.
Pole A scores toward -1, Pole B toward +1.
"""

SCHRODERS_V2_DIMENSIONS = [
    'b01', 'b02', 'b03', 'b04', 'b05', 'b06',
    'b07', 'b08', 'b09', 'b10', 'b11', 'b12',
]

SCHRODERS_V2_DIM_INFO = {
    'b01': {
        'title': 'Short-term \u2194 Long-term',
        'left_label': 'Short-term / quarterly',
        'right_label': 'Long-term / patient',
        'description': 'Whether the company chases short-term results (lower score) or plans patiently for the long term (higher score).',
        'thesis': 'Long-term orientation predicts capex quality and R&D productivity.',
    },
    'b02': {
        'title': 'Cost-focused \u2194 Growth-focused',
        'left_label': 'Cost-focused / thrifty',
        'right_label': 'Growth-focused / investing',
        'description': 'Whether the company prioritises cutting costs (lower score) or investing in growth (higher score). Both can be legitimate.',
        'thesis': 'Alignment with value vs growth strategy.',
    },
    'b03': {
        'title': 'Hierarchical \u2194 Egalitarian',
        'left_label': 'Hierarchical / top-down',
        'right_label': 'Egalitarian / distributed',
        'description': 'Whether decisions flow top-down through layers (lower score) or power is shared more equally (higher score).',
        'thesis': 'Egalitarian predicts faster decisions and retention in knowledge industries.',
    },
    'b04': {
        'title': 'Rules-driven \u2194 Judgement-driven',
        'left_label': 'Rules-driven / process',
        'right_label': 'Judgement-driven / adaptive',
        'description': 'Whether work is governed by defined processes and rules (lower score) or by individual judgement and adaptability (higher score).',
        'thesis': 'Rules-driven suits regulated industries; judgement-driven suits volatile ones.',
    },
    'b05': {
        'title': 'Individual \u2194 Team performance',
        'left_label': 'Individual performance',
        'right_label': 'Team performance',
        'description': 'Whether performance is about individual achievement (lower score) or collective teamwork (higher score).',
        'thesis': 'Individual outperformance for sales; team outperformance for engineering.',
    },
    'b06': {
        'title': 'Insular \u2194 Externally-focused',
        'left_label': 'Insular / internally-driven',
        'right_label': 'Externally-focused / market-driven',
        'description': 'Whether the company looks inward at itself (lower score) or outward at customers and markets (higher score).',
        'thesis': 'Externally-focused predicts better response to disruption.',
    },
    'b07': {
        'title': 'Risk-averse \u2194 Risk-taking',
        'left_label': 'Risk-averse',
        'right_label': 'Risk-taking',
        'description': 'Whether the company avoids risk (lower score) or embraces experimentation and bold moves (higher score).',
        'thesis': 'Risk-taking gives growth optionality; risk-aversion gives drawdown protection.',
    },
    'b08': {
        'title': 'Political \u2194 Meritocratic',
        'left_label': 'Political / tenure-based',
        'right_label': 'Meritocratic / performance-based',
        'description': 'Whether advancement depends on politics and tenure (lower score) or on merit and performance (higher score).',
        'thesis': 'Meritocracy predicts talent retention and productivity.',
    },
    'b09': {
        'title': 'Toxic \u2194 Supportive',
        'left_label': 'Toxic / high-turnover',
        'right_label': 'Supportive / low-turnover',
        'description': 'Whether the environment is hostile and burns people out (lower score) or supports and retains them (higher score).',
        'thesis': 'Retention cost and hiring quality.',
    },
    'b10': {
        'title': 'Chaotic \u2194 Stable',
        'left_label': 'Chaotic / strategy churn',
        'right_label': 'Stable / consistent',
        'description': 'Whether direction changes constantly (lower score) or strategy and organisation are consistent (higher score).',
        'thesis': 'Execution quality.',
    },
    'b11': {
        'title': 'Compliance-minimising \u2194 Integrity-maximising',
        'left_label': 'Compliance-minimising',
        'right_label': 'Integrity-maximising',
        'description': 'Whether the company cuts ethical corners (lower score) or holds itself to high standards of integrity (higher score).',
        'thesis': 'Reduced regulatory and reputational tail risk.',
    },
    'b12': {
        'title': 'Homogeneous \u2194 Diverse & inclusive',
        'left_label': 'Homogeneous',
        'right_label': 'Diverse & inclusive',
        'description': 'Whether the workforce and leadership are uniform (lower score) or genuinely diverse and inclusive (higher score).',
        'thesis': 'DEI correlates with decision quality where evidence supports it.',
    },
}

# Pole A = 'negative' (scores toward -1), Pole B = 'positive' (toward +1)
SCHRODERS_V2_SEEDS = {
    'b01': {
        'negative': [
            'short term thinking', 'quarterly targets', 'short term results',
            'hit the numbers', 'monthly targets', 'short sighted',
            'quick wins', 'chasing quarterly numbers', 'short term focus',
            'no long term plan',
        ],
        'positive': [
            'long term vision', 'long term strategy', 'invests for the future',
            'patient capital', 'sustainable growth', 'long term thinking',
            'thinks long term', 'future focused', 'long term view',
            'builds for the long run',
        ],
    },
    'b02': {
        'negative': [
            'cost cutting', 'budget cuts', 'penny pinching', 'cheap',
            'understaffed to save money', 'cutting corners on cost',
            'frugal', 'tight budgets', 'headcount freeze', 'do more with less',
        ],
        'positive': [
            'investing in growth', 'rapid expansion', 'growing fast',
            'hiring aggressively', 'invests in new products', 'expanding into new markets',
            'growth mindset', 'well funded', 'invests in technology',
            'scaling the business',
        ],
    },
    'b03': {
        'negative': [
            'top down management', 'very hierarchical', 'layers of management',
            'chain of command', 'decisions made at the top', 'micromanagement',
            'bureaucratic hierarchy', 'senior leadership decides everything',
            'rigid hierarchy', 'know your place',
        ],
        'positive': [
            'flat structure', 'open door policy', 'everyone has a voice',
            'flat hierarchy', 'accessible leadership', 'empowered employees',
            'autonomy to make decisions', 'collaborative decision making',
            'leadership listens', 'egalitarian culture',
        ],
    },
    'b04': {
        'negative': [
            'strict processes', 'rigid procedures', 'red tape',
            'process heavy', 'everything needs approval', 'follow the process',
            'compliance driven', 'checklists for everything', 'inflexible rules',
            'by the book',
        ],
        'positive': [
            'use your judgement', 'flexible approach', 'adapt quickly',
            'freedom to decide', 'trusted to make decisions', 'pragmatic',
            'agile ways of working', 'common sense over process',
            'adapts to change', 'nimble',
        ],
    },
    'b05': {
        'negative': [
            'individual targets', 'competitive colleagues', 'every man for himself',
            'cutthroat competition', 'stack ranking', 'internal competition',
            'personal quotas', 'sink or swim', 'star culture', 'sharp elbows',
        ],
        'positive': [
            'great teamwork', 'team oriented', 'collaborative environment',
            'supportive colleagues', 'team spirit', 'work as a team',
            'cross functional collaboration', 'everyone helps each other',
            'collective success', 'team first',
        ],
    },
    'b06': {
        'negative': [
            'internal politics', 'inward looking', 'ivory tower',
            'out of touch with the market', 'navel gazing', 'internally focused',
            'resistant to outside ideas', 'not invented here', 'insular culture',
            'behind the times',
        ],
        'positive': [
            'customer focused', 'customer first', 'market driven',
            'listens to customers', 'customer obsessed', 'close to the market',
            'responsive to clients', 'customer centric', 'externally focused',
            'in tune with the industry',
        ],
    },
    'b07': {
        'negative': [
            'risk averse', 'afraid of change', 'plays it safe',
            'slow to innovate', 'fear of failure', 'conservative culture',
            'resistant to new ideas', 'never takes risks', 'cautious to a fault',
            'status quo',
        ],
        'positive': [
            'takes risks', 'encourages experimentation', 'fail fast',
            'innovative culture', 'bold decisions', 'willing to try new things',
            'entrepreneurial spirit', 'pushes boundaries', 'embraces change',
            'test and learn',
        ],
    },
    'b08': {
        'negative': [
            'office politics', 'who you know', 'favoritism',
            'promotions based on tenure', 'brown nosing', 'political culture',
            'nepotism', 'boys club', 'play the game to get ahead',
            'seniority over merit',
        ],
        'positive': [
            'meritocracy', 'promotions based on performance', 'rewarded for results',
            'fair promotion process', 'best ideas win', 'performance based pay',
            'recognition for good work', 'merit based', 'earn your advancement',
            'talent gets recognised',
        ],
    },
    'b09': {
        'negative': [
            'toxic culture', 'toxic environment', 'toxic workplace',
            'hostile management', 'bullying', 'high turnover',
            'burnout culture', 'no support', "management doesn't care",
            'people leave constantly',
        ],
        'positive': [
            'supportive culture', 'supportive team', 'management cares',
            'psychologically safe', 'healthy environment', 'low turnover',
            'strong support system', 'work-life balance respected',
            'compassionate leadership', 'people stay for years',
        ],
    },
    'b10': {
        'negative': [
            'constant reorganisation', 'strategy changes every year', 'chaotic',
            'constant restructuring', 'no clear direction', 'shifting priorities',
            'disorganised', 'leadership churn', 'flavor of the month',
            'left hand doesn\'t know what the right is doing',
        ],
        'positive': [
            'stable company', 'consistent strategy', 'clear direction',
            'well organised', 'steady leadership', 'reliable employer',
            'consistent priorities', 'stable management', 'sticks to the plan',
            'predictable and dependable',
        ],
    },
    'b11': {
        'negative': [
            'cutting corners', 'unethical practices', 'shady',
            'cover up', 'dishonest management', 'questionable ethics',
            'pressure to bend the rules', 'regulatory issues', 'misleading customers',
            'look the other way',
        ],
        'positive': [
            'high integrity', 'ethical company', 'does the right thing',
            'honest leadership', 'strong values', 'transparent management',
            'holds itself accountable', 'ethical standards', 'trustworthy',
            'integrity matters here',
        ],
    },
    'b12': {
        'negative': [
            'lack of diversity', 'old boys club', 'all male leadership',
            'homogeneous workforce', 'glass ceiling', 'not inclusive',
            'diversity is lip service', 'no women in leadership',
            'tokenism', 'clique culture',
        ],
        'positive': [
            'diverse workforce', 'inclusive culture', 'diversity and inclusion',
            'women in leadership', 'welcoming to everyone', 'inclusive environment',
            'diverse leadership team', 'belonging', 'equal opportunities',
            'celebrates differences',
        ],
    },
}
