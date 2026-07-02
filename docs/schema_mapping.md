\# Schema Mapping



\## Staging Table



Target table:



`staging.stg\_food\_delivery\_orders`



\## Column Mapping



| Source Column | Staging Column |

|---|---|

| Restaurant ID | restaurant\_id |

| Restaurant name | restaurant\_name |

| Subzone | subzone |

| City | city |

| Order ID | order\_id |

| Order Placed At | order\_placed\_at |

| Order Status | order\_status |

| Delivery | delivery |

| Distance | distance |

| Items in order | items\_in\_order |

| Instructions | instructions |

| Discount construct | discount\_construct |

| Bill subtotal | bill\_subtotal |

| Packaging charges | packaging\_charges |

| Restaurant discount (Promo) | restaurant\_discount\_promo |

| Restaurant discount (Flat offs, Freebies \& others) | restaurant\_discount\_flat\_offs\_freebies\_others |

| Gold discount | gold\_discount |

| Brand pack discount | brand\_pack\_discount |

| Total | total |

| Rating | rating |

| Review | review |

| Cancellation / Rejection reason | cancellation\_rejection\_reason |

| Restaurant compensation (Cancellation) | restaurant\_compensation\_cancellation |

| Restaurant penalty (Rejection) | restaurant\_penalty\_rejection |

| KPT duration (minutes) | kpt\_duration\_minutes |

| Rider wait time (minutes) | rider\_wait\_time\_minutes |

| Order Ready Marked | order\_ready\_marked |

| Customer complaint tag | customer\_complaint\_tag |

| Customer ID | customer\_id |



\## Metadata Columns



| Column | Description |

|---|---|

| source\_file\_name | Name of the source CSV file |

| load\_date | Logical load date from the pipeline |

| loaded\_at | Timestamp when records were loaded into Azure SQL |

