# What Is Anycast?

**Anycast** is a networking technique in which multiple servers or data centers in different locations advertise the same IP address.

When a user connects to that IP address, internet routing—usually Border Gateway Protocol (BGP)—directs the request to the best available location according to network routing. This is not necessarily the location that is geographically closest.

```text
                    Same IP: 203.0.113.10
                   /          |           \
              New York      London      Singapore
                  ↑
            User routed here
```

## Benefits

- **Lower latency:** Users generally reach a nearby network location.
- **Higher availability:** If one location stops advertising the route, traffic can shift to another location.
- **DDoS resistance:** Attack traffic can be distributed across many locations instead of overwhelming one data center.
- **Stable DNS:** The public IP address can remain unchanged while servers and data centers behind it change.
- **Global traffic distribution:** Routing decisions occur at the network layer.

## Common Uses

Anycast is commonly used by:

- DNS providers
- Content delivery networks (CDNs)
- Global load balancers
- DDoS-protection services
- Public-cloud edge networks

For example, a DNS resolver can expose one IP address worldwide. A user in New Jersey may reach a US facility, while a user in Japan may reach an Asian facility, even though both users connect to the same IP address.

## Anycast vs. Traditional Load Balancing

| Anycast | Traditional load balancer |
| --- | --- |
| The same IP address is advertised from multiple locations | Usually provides one regional endpoint |
| Internet routing selects the location | Load-balancing software selects a server |
| Operates at the network-routing layer | Commonly operates at the transport or application layer |
| Best suited to global routing and resilience | Best suited to distributing traffic within a region |

The two techniques are frequently used together:

```text
User -> Anycast IP -> nearest network region -> regional load balancer -> server
```

## Important Limitation

Anycast selects the best **network route**, not necessarily the geographically closest or least-loaded server. Routing can also change while a user is connected. Applications must therefore handle traffic moving between locations, especially when they maintain stateful sessions.
