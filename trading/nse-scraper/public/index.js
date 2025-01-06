const sio = io(
    {
        transportOptions: {
            // polling: {
            //     extraHeaders: {
            //         'X-Username': window.location.hash.substring(1)
            //     }
            // }
        }
    }
);

function subscribe() {sio.on('connect', () => {
    console.log('connected to the server');
    console.log('socket id:', sio.id);
});

sio.on('connect_error', (e) => {
    console.log('connection error:', e.message);
    console.log('error details:', e);
});

sio.on('disconnect', () => {
    console.log('disconnected from the server');
    console.log('reason:', sio.disconnected);
});
    sio.disconnect()
    sio.connect()
    const feeds = [];
    if (document.getElementById('feed1').checked) feeds.push('35005');
    if (document.getElementById('feed2').checked) feeds.push('35025');
    if (document.getElementById('feed3').checked) feeds.push('35426');
    if (document.getElementById('feed4').checked) feeds.push('35004');
    if (document.getElementById('feed4').checked) feeds.push('1146913');
    sio.emit('subscribe', feeds);
}


sio.on('connect', () => {
    console.log('connected');
});



sio.on('connect_error', (e) => {
    console.log(e.message);
});

sio.on('disconnect', () => {
    console.log('disconnected');
});

// sio.on('market_data', (data) => {
//     console.log(data);
// });

document.addEventListener('DOMContentLoaded', () => {
    const instrumentCards = {
        '35005': document.getElementById('instrument1'),
        '35025': document.getElementById('instrument2'),
        '35426': document.getElementById('instrument3'),
        '35004': document.getElementById('instrument4'),
        '1126233': document.getElementById('instrument5'),
    };

    const createCardContent = (data) => {
        return `
            <h2>${data.name}</h2>
            <table>
                <tr><th>CMP</th><td>${data.cmp}</td></tr>
                <tr><th>Net Change</th><td class="${data.net_change >= 0 ? 'positive' : 'negative'}">${data.net_change}</td></tr>
                <tr><th>% Change</th><td class="${data.percent_change >= 0 ? 'positive' : 'negative'}">${(data.percent_change * 100).toFixed(2)} %</td></tr>
                <tr><th>VWAP</th><td class="${data.vwap >= 0 ? 'positive' : 'negative'}">${data.vwap}</td></tr>
                <tr><th>OI</th><td>${data.oi}</td></tr>
                <tr><th>OI Day High</th><td>${data.oi_day_high}</td></tr>
                <tr><th>OI Day Low</th><td>${data.oi_day_low}</td></tr>
                <tr><th>Net Demand/Supply</th><td class="${data.net_demand_supply >= 0 ? 'positive' : 'negative'}">${data.net_demand_supply}</td></tr>
                <tr><th>Ratio</th><td class="${data.ratio >= 1 ? 'positive' : 'negative'}">${data.ratio.toFixed(2)}</td></tr>
                <tr><th>Lot</th><td class="${data.lot >= 0 ? 'positive' : 'negative'}">${data.lot}</td></tr>
                <tr><th>Open</th><td>${data.open}</td></tr>
                <tr><th>High</th><td>${data.high}</td></tr>
                <tr><th>Low</th><td>${data.low}</td></tr>
            </table>
        `;
    };

    sio.on('market_data', (data) => {
        console.log('Market data received:', data);
        // Assuming data has 'instrument_id' to identify which card to update
        const instrumentId = `${data.security_id}`;
        if (instrumentCards[instrumentId]) {
            instrumentCards[instrumentId].innerHTML = createCardContent(data);
        }
    });
});
