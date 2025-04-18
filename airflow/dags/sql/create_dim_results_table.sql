SELECT 'win' AS code, 'Win' AS outcome, 'Win' AS description
        UNION ALL
        SELECT 'checkmated', 'Loss', 'Checkmated'
        UNION ALL
        SELECT 'agreed', 'Draw', 'Draw agreed'
        UNION ALL
        SELECT 'repetition', 'Draw', 'Draw by repetition'
        UNION ALL
        SELECT 'timeout', 'Win', 'Timeout'
        UNION ALL
        SELECT 'resigned', 'Loss', 'Resigned'
        UNION ALL
        SELECT 'stalemate', 'Draw', 'Stalemate'
        UNION ALL
        SELECT 'lose', 'Loss', 'Lose'
        UNION ALL
        SELECT 'insufficient', 'Draw', 'Insufficient material'
        UNION ALL
        SELECT '50move', 'Draw', 'Draw by 50-move rule'
        UNION ALL
        SELECT 'abandoned', 'Draw', 'Abandoned'
        UNION ALL
        SELECT 'kingofthehill', 'Win', 'Opponent king reached the hill'
        UNION ALL
        SELECT 'threecheck', 'Win', 'Checked for the 3rd time'
        UNION ALL
        SELECT 'timevsinsufficient', 'Draw', 'Draw by timeout vs insufficient material'
        UNION ALL
        SELECT 'bughousepartnerlose', 'Loss', 'Bughouse partner lost'