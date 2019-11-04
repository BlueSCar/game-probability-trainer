module.exports = (db) => {
    const synaptic = require('synaptic');
    const Architect = synaptic.Architect;
    const network = Architect.Perceptron.fromJSON(require('../networks/0.07496619140431109'));
    const fs = require('fs');

    const normalizeData = (data) => {
        let inputData = data.map(d => d.inputs);
        data.inputMaxes = [];
        data.inputMins = [];

        for (let i = 0; i < inputData[0].length; i++) {
            let values = inputData.map(d => d[i]);
            let max = Math.max(...values);
            let min = Math.min(...values);

            data.inputMaxes.push(max);
            data.inputMins.push(min);
        }

        let outputData = data.map(d => d.outputs);
        data.outputMaxes = [];
        data.outputMins = [];

        for (let i = 0; i < outputData[0].length; i++) {
            let values = outputData.map(d => d[i]);
            let max = Math.max(...values);
            let min = Math.min(...values);

            data.outputMaxes.push(max);
            data.outputMins.push(min);
        }

        for (let datum of data) {
            let inputNorm = [];
            for (let i = 0; i < datum.inputs.length; i++) {
                inputNorm.push((datum.inputs[i] - data.inputMins[i]) / (data.inputMaxes[i] - data.inputMins[i]));
            }

            datum.normalizedInput = inputNorm;

            let outputsNorm = [];
            for (let i = 0; i < datum.outputs.length; i++) {
                outputsNorm.push((datum.outputs[i] - data.outputMins[i]) / (data.outputMaxes[i] - data.outputMins[i]));
            }

            datum.normalizedOutputs = outputsNorm;
        }

        return {
            input: {
                mins: data.inputMins,
                maxes: data.inputMaxes
            },
            output: {
                mins: data.outputMins,
                maxes: data.outputMaxes
            }
        }
    }

    const generateTrainingData = async () => {
        const results = await db.any(`
        WITH plays AS (
            SELECT  g.id,
                    g.season,
                    g.week,
                    gt.team_id AS home_id,
                    gt2.team_id AS away_id,
                    gt.winner AS home_winner,
                    CASE WHEN gt.team_id = p.offense_id THEN true ELSE false END AS home,
                    CASE
                        WHEN p.down = 2 AND p.distance >= 8 THEN 'passing'
                        WHEN p.down IN (3,4) AND p.distance >= 5 THEN 'passing'
                        ELSE 'standard'
                    END AS down_type,
                    CASE
                        WHEN p.scoring = true THEN true
                        WHEN p.down = 1 AND (CAST(p.yards_gained AS NUMERIC) / p.distance) >= 0.5 THEN true
                        WHEN p.down = 2 AND (CAST(p.yards_gained AS NUMERIC) / p.distance) >= 0.7 THEN true
                        WHEN p.down IN (3,4) AND (p.yards_gained >= p.distance) THEN true
                        ELSE false
                    END AS success,
                    CASE 
                        WHEN p.play_type_id IN (3,4,6,7,24,26,36,51,67) THEN 'Pass'
                        WHEN p.play_type_id IN (5,9,29,39,68) THEN 'Rush'
                        ELSE 'Other'
                    END AS play_type,
                    CASE
                        WHEN p.period = 2 AND p.scoring = false AND ABS(p.home_score - p.away_score) > 38 THEN true
                        WHEN p.period = 3 AND p.scoring = false AND ABS(p.home_score - p.away_score) > 28 THEN true
                        WHEN p.period = 4 AND p.scoring = false AND ABS(p.home_score - p.away_score) > 22 THEN true
                        WHEN p.period = 2 AND p.scoring = true AND ABS(p.home_score - p.away_score) > 45 THEN true
                        WHEN p.period = 3 AND p.scoring = true AND ABS(p.home_score - p.away_score) > 35 THEN true
                        WHEN p.period = 4 AND p.scoring = true AND ABS(p.home_score - p.away_score) > 29 THEN true
                        ELSE false
                    END AS garbage_time,
                    p.period,
                    p.ppa AS ppa
            FROM game AS g
                INNER JOIN game_team AS gt ON g.id = gt.game_id AND gt.home_away = 'home'
                INNER JOIN game_team AS gt2 ON g.id = gt2.game_id AND gt.id <> gt2.id
                INNER JOIN drive AS d ON g.id = d.game_id
                INNER JOIN play AS p ON d.id = p.drive_id AND p.ppa IS NOT NULL
            WHERE g.season > 2013 AND g.season < 2019
        )
        SELECT 	id,
                season,
                week,
                home_winner,
                AVG(ppa) FILTER(WHERE home = true) AS ppa,
                SUM(ppa) FILTER(WHERE home = true) AS total_ppa,
                AVG(ppa) FILTER(WHERE home = true AND play_type = 'Pass') AS passing_ppa,
                AVG(ppa) FILTER(WHERE home = true AND play_type = 'Rush') AS rushing_ppa,
                AVG(ppa) FILTER(WHERE home = true AND down_type = 'standard') AS standard_down_ppa,
                AVG(ppa) FILTER(WHERE home = true AND down_type = 'passing') AS passing_down_ppa,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true) AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true), 0), 1) AS success_rate,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true AND down_type = 'standard') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true AND down_type = 'standard'), 0), 1) AS standard_success_rate,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true AND down_type = 'passing') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true AND down_type = 'passing'), 0), 1) AS passing_success_rate,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true AND play_type = 'Rush') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true AND play_type = 'Rush'), 0), 1) AS rushing_success,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true AND play_type = 'Pass') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true AND play_type = 'Pass'), 0), 1) AS passing_success,
                COALESCE(AVG(ppa) FILTER(WHERE home = true AND success = true), 0) AS explosiveness,
                COALESCE(AVG(ppa) FILTER(WHERE home = true AND success = true AND play_type = 'Pass'), 0) AS pass_explosiveness,
                COALESCE(AVG(ppa) FILTER(WHERE home = true AND success = true AND play_type = 'Rush'), 0) AS rush_explosiveness,
                CAST(COUNT(*) FILTER(WHERE home = true AND play_type = 'Pass') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true), 0), 1) AS pass_rate,
                AVG(ppa) FILTER(WHERE home = false) AS ppa2,
                SUM(ppa) FILTER(WHERE home = false) AS total_ppa2,
                COALESCE(AVG(ppa) FILTER(WHERE home = false AND play_type = 'Pass'), 0) AS passing_ppa2,
                AVG(ppa) FILTER(WHERE home = false AND play_type = 'Rush') AS rushing_ppa2,
                AVG(ppa) FILTER(WHERE home = false AND down_type = 'standard') AS standard_down_ppa2,
                AVG(ppa) FILTER(WHERE home = false AND down_type = 'passing') AS passing_down_ppa2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true) AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false), 0), 1) AS success_rate2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true AND down_type = 'standard') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false AND down_type = 'standard'), 0), 1) AS standard_success_rate2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true AND down_type = 'passing') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false AND down_type = 'passing'), 0), 1) AS passing_success_rate2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true AND play_type = 'Rush') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false AND play_type = 'Rush'), 0), 1) AS rushing_success2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true AND play_type = 'Pass') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false AND play_type = 'Pass'), 0), 1) AS passing_success2,
                COALESCE(AVG(ppa) FILTER(WHERE home = false AND success = true), 0) AS explosiveness2,
                COALESCE(AVG(ppa) FILTER(WHERE home = false AND success = true AND play_type = 'Pass'), 0) AS pass_explosiveness2,
                COALESCE(AVG(ppa) FILTER(WHERE home = false AND success = true AND play_type = 'Rush'), 0) AS rush_explosiveness2,
                CAST(COUNT(*) FILTER(WHERE home = false AND play_type = 'Pass') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false), 0), 1) AS pass_rate2
        FROM plays
        WHERE garbage_time = false
        GROUP BY id, season, week, home_winner
        HAVING COUNT(*) FILTER(WHERE home = false) > 0 AND COUNT(*) FILTER(WHERE home = true) > 0
        `);

        const trainingData = results.map(r => {
            return {
                id: r.id,
                year: parseInt(r.season),
                week: parseInt(r.week),
                inputs: [
                    parseFloat(r.ppa),
                    parseFloat(r.total_ppa),
                    parseFloat(r.passing_ppa),
                    parseFloat(r.rushing_ppa),
                    parseFloat(r.standard_down_ppa),
                    parseFloat(r.passing_down_ppa),
                    parseFloat(r.success_rate),
                    parseFloat(r.standard_success_rate),
                    parseFloat(r.passing_success_rate),
                    parseFloat(r.passing_success),
                    parseFloat(r.rushing_success),
                    parseFloat(r.explosiveness),
                    parseFloat(r.pass_explosiveness),
                    parseFloat(r.rush_explosiveness),
                    parseFloat(r.pass_rate),
                    parseFloat(r.ppa2),
                    parseFloat(r.total_ppa2),
                    parseFloat(r.passing_ppa2),
                    parseFloat(r.rushing_ppa2),
                    parseFloat(r.standard_down_ppa2),
                    parseFloat(r.passing_down_ppa2),
                    parseFloat(r.success_rate2),
                    parseFloat(r.standard_success_rate2),
                    parseFloat(r.passing_success_rate2),
                    parseFloat(r.passing_success2),
                    parseFloat(r.rushing_success2),
                    parseFloat(r.explosiveness2),
                    parseFloat(r.pass_explosiveness2),
                    parseFloat(r.rush_explosiveness2),
                    parseFloat(r.pass_rate2)
                ],
                outputs: [
                    r.home_winner ? 1 : 0
                ]
            }
        });
        const keys = normalizeData(trainingData);

        fs.writeFileSync('./keys.json', JSON.stringify(keys, null, '\t'));
        fs.writeFileSync('./training.json', JSON.stringify(trainingData.filter(d => d.year % 2 === d.week % 2).map(d => ({
            input: d.normalizedInput,
            output: d.normalizedOutputs
        })), null, '\t'));
        fs.writeFileSync('./testing.json', JSON.stringify(trainingData.filter(d => d.year % 2 !== d.week % 2).map(d => ({
            input: d.normalizedInput,
            output: d.normalizedOutputs
        })), null, '\t'));
    };

    const renormalizeData = (data) => {
        let keys = require('../keys');
        let inputMins = keys.input.mins;
        let inputMaxes = keys.input.maxes;
        let inputNorm = [];
        for (let i = 0; i < data.inputs.length; i++) {
            inputNorm.push((data.inputs[i] - inputMins[i]) / (inputMaxes[i] - inputMins[i]));
        }

        // datum.normalizedInput = inputNorm;

        return inputNorm;
    };

    const evaluateGame = async (id) => {
        const r = await db.oneOrNone(`
        WITH plays AS (
            SELECT  g.id,
                    g.season,
                    g.week,
                    gt.team_id AS home_id,
                    gt2.team_id AS away_id,
                    gt.winner AS home_winner,
                    CASE WHEN gt.team_id = p.offense_id THEN true ELSE false END AS home,
                    CASE
                        WHEN p.down = 2 AND p.distance >= 8 THEN 'passing'
                        WHEN p.down IN (3,4) AND p.distance >= 5 THEN 'passing'
                        ELSE 'standard'
                    END AS down_type,
                    CASE
                        WHEN p.scoring = true THEN true
                        WHEN p.down = 1 AND (CAST(p.yards_gained AS NUMERIC) / p.distance) >= 0.5 THEN true
                        WHEN p.down = 2 AND (CAST(p.yards_gained AS NUMERIC) / p.distance) >= 0.7 THEN true
                        WHEN p.down IN (3,4) AND (p.yards_gained >= p.distance) THEN true
                        ELSE false
                    END AS success,
                    CASE 
                        WHEN p.play_type_id IN (3,4,6,7,24,26,36,51,67) THEN 'Pass'
                        WHEN p.play_type_id IN (5,9,29,39,68) THEN 'Rush'
                        ELSE 'Other'
                    END AS play_type,
                    CASE
                        WHEN p.period = 2 AND p.scoring = false AND ABS(p.home_score - p.away_score) > 38 THEN true
                        WHEN p.period = 3 AND p.scoring = false AND ABS(p.home_score - p.away_score) > 28 THEN true
                        WHEN p.period = 4 AND p.scoring = false AND ABS(p.home_score - p.away_score) > 22 THEN true
                        WHEN p.period = 2 AND p.scoring = true AND ABS(p.home_score - p.away_score) > 45 THEN true
                        WHEN p.period = 3 AND p.scoring = true AND ABS(p.home_score - p.away_score) > 35 THEN true
                        WHEN p.period = 4 AND p.scoring = true AND ABS(p.home_score - p.away_score) > 29 THEN true
                        ELSE false
                    END AS garbage_time,
                    p.period,
                    p.ppa AS ppa
            FROM game AS g
                INNER JOIN game_team AS gt ON g.id = gt.game_id AND gt.home_away = 'home'
                INNER JOIN game_team AS gt2 ON g.id = gt2.game_id AND gt.id <> gt2.id
                INNER JOIN drive AS d ON g.id = d.game_id
                INNER JOIN play AS p ON d.id = p.drive_id AND p.ppa IS NOT NULL
            WHERE g.id = $1
        )
        SELECT 	id,
                season,
                week,
                home_winner,
                AVG(ppa) FILTER(WHERE home = true) AS ppa,
                SUM(ppa) FILTER(WHERE home = true) AS total_ppa,
                AVG(ppa) FILTER(WHERE home = true AND play_type = 'Pass') AS passing_ppa,
                AVG(ppa) FILTER(WHERE home = true AND play_type = 'Rush') AS rushing_ppa,
                AVG(ppa) FILTER(WHERE home = true AND down_type = 'standard') AS standard_down_ppa,
                AVG(ppa) FILTER(WHERE home = true AND down_type = 'passing') AS passing_down_ppa,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true) AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true), 0), 1) AS success_rate,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true AND down_type = 'standard') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true AND down_type = 'standard'), 0), 1) AS standard_success_rate,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true AND down_type = 'passing') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true AND down_type = 'passing'), 0), 1) AS passing_success_rate,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true AND play_type = 'Rush') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true AND play_type = 'Rush'), 0), 1) AS rushing_success,
                CAST(COUNT(*) FILTER(WHERE home = true AND success = true AND play_type = 'Pass') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true AND play_type = 'Pass'), 0), 1) AS passing_success,
                COALESCE(AVG(ppa) FILTER(WHERE home = true AND success = true), 0) AS explosiveness,
                COALESCE(AVG(ppa) FILTER(WHERE home = true AND success = true AND play_type = 'Pass'), 0) AS pass_explosiveness,
                COALESCE(AVG(ppa) FILTER(WHERE home = true AND success = true AND play_type = 'Rush'), 0) AS rush_explosiveness,
                CAST(COUNT(*) FILTER(WHERE home = true AND play_type = 'Pass') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = true), 0), 1) AS pass_rate,
                AVG(ppa) FILTER(WHERE home = false) AS ppa2,
                SUM(ppa) FILTER(WHERE home = false) AS total_ppa2,
                COALESCE(AVG(ppa) FILTER(WHERE home = false AND play_type = 'Pass'), 0) AS passing_ppa2,
                AVG(ppa) FILTER(WHERE home = false AND play_type = 'Rush') AS rushing_ppa2,
                AVG(ppa) FILTER(WHERE home = false AND down_type = 'standard') AS standard_down_ppa2,
                AVG(ppa) FILTER(WHERE home = false AND down_type = 'passing') AS passing_down_ppa2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true) AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false), 0), 1) AS success_rate2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true AND down_type = 'standard') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false AND down_type = 'standard'), 0), 1) AS standard_success_rate2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true AND down_type = 'passing') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false AND down_type = 'passing'), 0), 1) AS passing_success_rate2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true AND play_type = 'Rush') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false AND play_type = 'Rush'), 0), 1) AS rushing_success2,
                CAST(COUNT(*) FILTER(WHERE home = false AND success = true AND play_type = 'Pass') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false AND play_type = 'Pass'), 0), 1) AS passing_success2,
                COALESCE(AVG(ppa) FILTER(WHERE home = false AND success = true), 0) AS explosiveness2,
                COALESCE(AVG(ppa) FILTER(WHERE home = false AND success = true AND play_type = 'Pass'), 0) AS pass_explosiveness2,
                COALESCE(AVG(ppa) FILTER(WHERE home = false AND success = true AND play_type = 'Rush'), 0) AS rush_explosiveness2,
                CAST(COUNT(*) FILTER(WHERE home = false AND play_type = 'Pass') AS NUMERIC) / COALESCE(NULLIF(COUNT(*) FILTER(WHERE home = false), 0), 1) AS pass_rate2
        FROM plays
        WHERE garbage_time = false
        GROUP BY id, season, week, home_winner
        HAVING COUNT(*) FILTER(WHERE home = false) > 0 AND COUNT(*) FILTER(WHERE home = true) > 0
        `, [id]);

        if (!r) {
            return null;
        }

        const trainingData = {
            id: r.id,
            year: parseInt(r.season),
            week: parseInt(r.week),
            inputs: [
                parseFloat(r.ppa),
                parseFloat(r.total_ppa),
                parseFloat(r.passing_ppa),
                parseFloat(r.rushing_ppa),
                parseFloat(r.standard_down_ppa),
                parseFloat(r.passing_down_ppa),
                parseFloat(r.success_rate),
                parseFloat(r.standard_success_rate),
                parseFloat(r.passing_success_rate),
                parseFloat(r.passing_success),
                parseFloat(r.rushing_success),
                parseFloat(r.explosiveness),
                parseFloat(r.pass_explosiveness),
                parseFloat(r.rush_explosiveness),
                parseFloat(r.pass_rate),
                parseFloat(r.ppa2),
                parseFloat(r.total_ppa2),
                parseFloat(r.passing_ppa2),
                parseFloat(r.rushing_ppa2),
                parseFloat(r.standard_down_ppa2),
                parseFloat(r.passing_down_ppa2),
                parseFloat(r.success_rate2),
                parseFloat(r.standard_success_rate2),
                parseFloat(r.passing_success_rate2),
                parseFloat(r.passing_success2),
                parseFloat(r.rushing_success2),
                parseFloat(r.explosiveness2),
                parseFloat(r.pass_explosiveness2),
                parseFloat(r.rush_explosiveness2),
                parseFloat(r.pass_rate2)
            ]
        };

        const normalized = renormalizeData(trainingData);
        const normalizedResult = network.activate(normalized);

        return normalizedResult[0];
    };

    return {
        generateTrainingData,
        evaluateGame
    };
};