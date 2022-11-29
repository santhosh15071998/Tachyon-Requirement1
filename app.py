from flask import Flask, request,jsonify
import controller as dynamodb

app = Flask(__name__)


@app.route('/test')
def test():
    print("TEST SUCCESS")
    return 'TEST SUCCESS'

data= {
"id": 222,
"data": {
  
  "background": "rgb(253,251,252)",
  "width": "100vw",
  "title": "Cognitive assistant platform",
  "header": {
    "logoUrl": "https://www.tachyonsys.com.au/img/logos/logo-white.png",
    "brandName": "Tachyon Systems",
    "background": "rgb(253,251,252)",
    "width": "100vw",
    "height": "80px",
    "navMenu": [
      {
        "name": "Products",
        "link": "section-1"
      },
      {
        "name": "Solutions",
        "link": "section-2"
      },
      {
        "name": "Features",
        "link": "section-3"
      },
      {
        "name": "Partners",
        "link": "section-4"
      },
      {
        "name": "Pricing",
        "link": "section-5"
      },
      {
        "name": "FAQ",
        "link": "section-6"
      },
      {
        "name": "About us",
        "link": "section-7"
      },
      {
        "name": "Contact us",
        "link": "section-8"
      }
    ]
  },
  "sections": [
    {
      "id": "hero",
      "title": "INNOVATE AND ADAPT FASTER",
      "content": "Tachyon is an award-winning cognitive assistant platform for Software Engineers to develop enterprise-grade quality software solutions swiftly with minimum efforts. Tachyon understands any human native language and converts software diagrams into relevant programs. Tachyon augments end-users by providing context-specific suggestions, recommendations, discovery, and intelligent automation.",
      "imageUrl": "",
      "background": "rgb(255,192,0)",
      "width": "100vw",
      "height": "100vh",
      "action": {
        "text": "No Credit cards. Sign up here for get demo link",
        "label": "Get Demo",
        "link": "https://www.tachyonsys.com.au/book-demo"
      }
    },
    {
      "id": "section-1",
      "title": "Our Products",
      "content": "Tachyon ( aka Tachy ) is an award-winning platform - Artificial Intelligence-based Rapid Application development platform which helps IT professionals, to design, develop and deploy enterprise-grade software applications with less effort in no time.",
      "imageUrl": "",
      "background": "rgb(251,229,214)",
      "width": "100vw",
      "height": "400px",
      "action": {
        "text": "Wanna to explore more details?",
        "label": "Explore more",
        "link": "https://www.tachyonsys.com.au/platform/overview"
      }
    },
    {
      "id": "section-2",
      "title": "Our Solutions",
      "content": "We aid to create new business value propositions from existing, aging applications by updating them with modern features and power. By migrating legacy applications, we help businesses to include the latest functionalities and align the market needs.",
      "imageUrl": "",
      "background": "rgb(222,235,247)",
      "width": "100vw",
      "height": "400px",
      "action": {
        "text": "Wanna to lean more details?",
        "label": "Learn more",
        "link": "https://www.tachyonsys.com.au/solutions/overview"
      }
    },
    {
      "id": "section-3",
      "title": "Features",
      "content": "TACHYON is embraced, created, and developed with strong intention of bringing a revolution in the software industry. It is one of its kind artificial intelligent voice-driven software development platforms",
      "imageUrl": "",
      "background": "rgb(208,206,206)",
      "width": "100vw",
      "height": "400px",
      "action": {
        "text": "Wanna to find more details?",
        "label": "Find more",
        "link": "https://www.tachyonsys.com.au/platform/features"
      }
    },
    {
      "id": "section-4",
      "title": "Our Partners",
      "content": "Many organizations bring value and opportunities for our customers. We've designed the Tachyon Partner Program with partnership categories, levels, criteria, and benefits tailored to meet each partner's business objectives. We are committed to providing our partners with the highest level of support to develop, market, sell, and deliver industry-leading solutions with Tachyon, while achieving the highest degree of customer success and partner satisfaction",
      "imageUrl": "",
      "background": "rgb(216,220,202)",
      "width": "100vw",
      "height": "500px",
      "action": {
        "text": "Wanna to become an partner?",
        "label": "Join",
        "link": "https://www.tachyonsys.com.au/partners/partner-program-overview"
      }
    },
    {
      "id": "section-5",
      "title": "Pricing",
      "content": "The time has come for you to pick up the phone and give us a call.",
      "imageUrl": "",
      "background": "rgb(254,250,240)",
      "width": "100vw",
      "height": "250px",
      "action": {
        "text": "Wanna to get prcing details",
        "label": "Get Pricing",
        "link": "https://www.tachyonsys.com.au/platform/pricing"
      }
    },
    {
      "id": "section-6",
      "title": "FAQ",
      "content": "We focus that our culture is innovative, flexible and overall the driving force for our employees to perform at their best. The hugely satisfying and impactful work processes at Tachyon acts as a magic component for designing our cultural values. Our culture is not just a platform created by us but reaped well with trust and commitment of our employees. Our employee hunt focuses on 3I’s to build a rich legacy of culture.",
      "imageUrl": "",
      "background": "rgb(249,253,233)",
      "width": "100vw",
      "height": "350px",
      "action": {
        "text": "For further frequently asked questions",
        "label": "More questions",
        "link": "https://www.tachyonsys.com.au/platform/pricing"
      }
    },
    {
      "id": "section-7",
      "title": "About us",
      "content": "Tachyon Systems is a Melbourne based technology company emerged on account of our leading-edge technology imagination and our strong in-house culture of innovation. Our objective is to be a digital companion for your software team and augment the SDLC process by aiding simplicity and invention across every stage of the software development life cycle. Tachyon platform offers versatility and uniformity for software discovery, design, development, deployment, and delivery.",
      "imageUrl": "",
      "background": "rgb(254,222,254)",
      "width": "100vw",
      "height": "200vh"
    },
    {
      "id": "section-8",
      "title": "Contact us",
      "content": "We are available 24/7 by e-mail and phone. You can also ask a question about our services through our contact form that we regularly provide.",
      "imageUrl": "",
      "background": "rgb(210,247,254)",
      "width": "100vw",
      "height": "200vh"
    }
  ],
  "footer": {
    "siteMap": 'true',
    "socialMedia": {
      "linkedin": "https://www.linkedin.com/company/tachyon-systems",
      "facebook": "https://www.facebook.com/Tachyon-106407487370446/",
      "twitter": "https://twitter.com/systemstachyon",
      "youtube": "https://www.youtube.com/channel/UCkw8wb4psCuzehnyRY3MNZg",
      "instagram": "https://www.instagram.com/tachyonsystems"
    },
    "privacy": "https://www.tachyonsys.com.au/privacy",
    "term": "https://www.tachyonsys.com.au/terms"
  }
}
}
  
@app.route('/')
def root_route():
    dynamodb.create_table_movie()
    return 'TACHYON-REQUIREMENT'

@app.route('/post', methods=['POST'])
def add_data():
    data = request.get_json()
    response = dynamodb.write_to_dynamodb(data['id'], data['data'])    
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        return {
            'msg': 'Added successfully',
        }
    return {  
        'msg': 'Some error occcured',
        'response': response
    }

@app.route('/get/<int:id>', methods=['GET'])
def get_data(id):
    response = dynamodb.read_from_dynamodb(id)
    if (response['ResponseMetadata']['HTTPStatusCode'] == 200):
        if ('Item' in response):
            return { 'Item': response['Item'] }
        return { 'msg' : 'Item not found!' }
    return {
        'msg': 'Some error occured',
        'response': response
    }

if __name__ == '__main__':
    app.run(host='127.0.0.1', port=5000, debug=True)