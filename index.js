const bluebird = require('bluebird');
const dotenv = require('dotenv');
const pgp = require('pg-promise');

const dbConfig = require('./lib/database');
const trainerConfig = require('./lib/trainer');

(async() => {
    dotenv.config();

    const db = dbConfig(bluebird, pgp);
    const trainer = trainerConfig(db);

    // await trainer.generateTrainingData();

    
    const games = await db.any(`
        SELECT g.id, gt.id AS game_team1_id, gt2.id AS game_team2_id
        FROM game AS g
            INNER JOIN game_team AS gt ON g.id = gt.game_id AND gt.home_away = 'home'
            INNER JOIN game_team AS gt2 ON g.id = gt2.game_id AND gt.id <> gt2.id
        WHERE g.season = 2015 AND gt.points IS NOT NULL AND gt2.points IS NOT NULL AND gt.win_prob IS NULL AND gt2.win_prob IS NULL
    `);

    for (let game of games) {
        let result = await trainer.evaluateGame(game.id);
        if (!result){
            continue;
        }

        await db.tx(async t => {
            await t.batch([
                t.none('UPDATE game_team SET win_prob = $1 WHERE id = $2', [result, game.game_team1_id]),
                t.none('UPDATE game_team SET win_prob = $1 WHERE id = $2', [1 - result, game.game_team2_id])
            ]);
        });
    }
})().catch(err => {
    console.error(err);
});