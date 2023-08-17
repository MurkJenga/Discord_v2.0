import discord, asyncio
from discord import app_commands  
from dotenv import dotenv_values   
import mysql.connector 
from columnar import columnar
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine
from plexapi.myplex import MyPlexAccount 
import random  
import openai

intents = discord.Intents.all()
vars = dotenv_values(".env")
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)
guildId = vars['GuildId']  #<--Will control which channel to point to 
token = vars["TOKEN"]  

account = MyPlexAccount(vars["EMAIL"], vars["PASSWORD"])
plex = account.resource(vars["PLEXSERVER"]).connect()

config = { #Secure code using environmental variables
    'user': vars["USERNAME"],
    'password': vars["PASSWORD"],
    'host': vars["HOSTNAME"],
    'database': vars["DATABASE"],
    'raise_on_warnings': True,
    'autocommit': True
}

cnx = mysql.connector.connect(**config) #connection
cursor = cnx.cursor()

def exec(query): #Establish query executor
    try:
        cursor.execute(query)
        return cursor.fetchall()
    except Exception as e:
        return str(e)

async def dataReturn(output, emoji, interaction, title): #Abstracting out to reuse in functions later that return row based data
    data = []
    if not output:
        await interaction.response.send_message(embed=discord.Embed(title=f"These is no data for {emoji}", color=discord.Color.blue()))
    elif isinstance(output, list):
            for result in output:
                sub_data = []
                for x in result:
                    sub_data.append(x)
                data.append(sub_data) 
            await interaction.response.send_message(embed=discord.Embed(title=title, description=columnar(data , no_borders=True), color=discord.Color.purple()))
            return  

############################################################################ Slash Commands ############################################################################

@tree.command(name = "total", description = "Return number of messages sent by each user.", guild=discord.Object(id=guildId)) #Return total messages
async def given(interaction): 
        output = exec("select * from Total_Messages_vw") 
        title=f"Total messages per user"
        emoji = 'now' 
        try: 
            await dataReturn(output, emoji, interaction, title)  
        except Exception as e:
            return str(e)

@tree.command(name = "given", description = "Return number of times a user has given a specific emoji.", guild=discord.Object(id=guildId)) #Return Given Emoji
@app_commands.describe(emoji = 'Enter Emoji')
async def given(interaction, emoji:str):
    print(f'{interaction.user} used "given" {emoji}')
    if "'" not in emoji:
        output = exec(f"""select 
                            concat(ROW_NUMBER() OVER (PARTITION BY `active` ORDER BY COUNT(1) DESC), '.') as 'Rank',
                            CONCAT(SUBSTRING(member_name FROM 1 FOR CHAR_LENGTH(member_name) - 5), ': ') as UserGaveEmojis,
                            count(1) as Total
                            from reactions_vw
                            where
                            active = 1 and emoji = '{emoji}'
                            group by member_name """) 
        title=f"Total {emoji} given for each user"
        try:
            await dataReturn(output, emoji, interaction, title) 
        except Exception as e:
            return str(e)
    else:
        await interaction.response.send_message(embed=discord.Embed(title=f"{interaction.user} used an invalid character", color=discord.Color.red())) 

@tree.command(name = "received", description = "Return number of times a user has received a specific emoji.", guild=discord.Object(id=guildId)) #Return received emoji
@app_commands.describe(emoji = 'Enter Emoji')
async def given(interaction, emoji:str):
    print(f'{interaction.user} used "received" {emoji}')
    if "'" not in emoji:
        output = exec(f"""select 
                            concat(ROW_NUMBER() OVER (PARTITION BY emoji ORDER BY COUNT(1) DESC), '.') as 'Rank',
                            CONCAT(SUBSTRING(m.author FROM 1 FOR CHAR_LENGTH(author) - 5), ': ') ,
                            count(1) as TotalEmojisRecieved
                            from lnd_messages m
                            join lnd_reaction r on r.message_id = m.id and active = 1
                            join lnd_users u on m.author = u.user_name and isbot = 0
                        where
                               r.emoji =  '{emoji}' 
                        group by m.author, emoji;""") 
        title=f"Total {emoji} received for each user"
        try:
            await dataReturn(output, emoji, interaction, title) 
        except Exception as e:
            return str(e)
    else:
        await interaction.response.send_message(embed=discord.Embed(title=f"{interaction.user} used an invalid character", color=discord.Color.red())) 

@tree.command(name = "user", description = "Return stats on given user.", guild=discord.Object(id=guildId)) #Return user stats
@app_commands.describe(user = 'Enter User')
async def given(interaction, user:discord.Member): 
    print(f'{interaction.user} used "user" {user}')
    query = f'select * from UserStats_vw where user_name = "{user}"'
    df = pd.read_sql(query,cnx) 
    df.style.hide_index() 
    embed = discord.Embed(title=f'{df.User.to_string(index=False)}\'s stats' , colour=discord.Colour.from_rgb(255, 0, 230))
    embed.add_field(name="Join Date", value=df.JoinDate.to_string(index=False), inline=True)
    embed.add_field(name="Total Messages", value=df.Total_Messages.to_string(index=False), inline=True)
    embed.add_field(name="Emojis Given", value=df.Total_Emojis_Given.to_string(index=False), inline=True)
    embed.add_field(name="Emojis Received", value=df.Total_Emojis_Received.to_string(index=False), inline=True)
    embed.add_field(name="Days In Server", value=df.Days_In_Server.to_string(index=False), inline=True)
    embed.add_field(name="Avg Messages/Day", value=df.Average_Messages_a_Day.to_string(index=False), inline=True)
    embed.add_field(name="Last Message", value=df.lastmsg.to_string(index=False), inline=True)
    embed.add_field(name="Top Emoji", value=df.Favorite_Emoji.to_string(index=False), inline=True)
    try:
        await interaction.response.send_message(embed=embed)
    except Exception as e:
            return str(e)
    
@tree.command(name = "channel", description = "Return stats on given channel.", guild=discord.Object(id=guildId)) #Return channel stats
@app_commands.describe(channel = 'Enter channel')
async def given(interaction, channel:discord.TextChannel): 
    print(f'{interaction.user} used "channel" {channel}')
    query = f"select * from channelstats_vw where name = '{channel}'"
    df = pd.read_sql(query,cnx) 
    df.style.hide_index() 
    embed = discord.Embed(title=f'{df.name.to_string(index=False)}\'s stats' , colour=discord.Colour.from_rgb(0, 255, 0))
    embed.add_field(name="Total Messages", value=df.totmsg.to_string(index=False), inline=True) 
    embed.add_field(name="Top Contributor", value=df.author.to_string(index=False), inline=True)
    embed.add_field(name="Top Emoji", value=df.emoji.to_string(index=False), inline=True)
    embed.add_field(name="Avg Message Per Day", value=df.perday.to_string(index=False), inline=True) 
    embed.add_field(name="Birth Date", value=df.created.to_string(index=False), inline=True) 
    try:
        await interaction.response.send_message(embed=embed)
    except Exception as e:
            return str(e)
    
@tree.command(name = "help", description = "Show available commands", guild=discord.Object(id=guildId)) #Return available commands
async def given(interaction):  
    description = """
            Try these \n\n
            total: Breakdown of total messages per user in all channels\n 
            user + user name: Lists user stats\n 
            channel + channel name: Lists Channel stats\n 
            received + emoji: Lists total times an emoji was recieved\n
            given + emoji: Lists total times an emoji was given\n 
            """
    try:
        await interaction.response.send_message(embed=discord.Embed(title='Use / with the commands below', description=description, color=discord.Color.yellow()))
    except Exception as e:
            return str(e)

@tree.command(name = "otl", description = "Reload the messages database", guild=discord.Object(id=guildId)) #Return total messages
async def otl(interaction): 
    engine = create_engine("mysql+pymysql://" + config['user'] + ":" + config['password'] + "@" + config['host'] + "/" + config['database'])
    if interaction.user.id == 553337834090659899:
        messageColumns = ['author', 'content', 'edited_dte', 'created_dte', 'server', 'id', 'channel', 'author_id']
        reactionColumns = ['message_id', 'emoji', 'member_name', 'channel_id', 'active']
        await interaction.response.send_message(embed=discord.Embed(title='I am loading the messages and emojis while I poop' , color=discord.Color.magenta()))
        message_lst = []
        reaction_lst = []
        for channel in interaction.guild.text_channels:
            async for message in channel.history(limit=None):
                if client.user.id != message.author.id:
                    try:
                        message_lst.append([str(message.author), str(message.content), str(message.edited_at), str(message.created_at), str(message.guild), message.id, str(message.channel), str(message.author.id)])
                    except Exception as e:
                        return str(e)
                    for reaction in message.reactions:
                        users = reaction.users()
                        async for u in users:
                            if u != client.user.name:
                                reaction_lst.append([message.id, str(reaction), str(u), message.channel.id, 1])
            print(f'Finished {channel}')
        df = pd.DataFrame(message_lst, columns=messageColumns)  
        df.to_sql(con=engine,name='lnd_messages',if_exists='append',index=False)

        reaction_df = pd.DataFrame(reaction_lst, columns=reactionColumns)
        reaction_df.to_sql(con=engine,name='lnd_reaction',if_exists='append',index=False)

        await interaction.channel.send(embed=discord.Embed(title="Im done pooping and loading" , color=discord.Color.green()))
    else:
        await interaction.response.send_message(embed=discord.Embed(title='Knock it off, I dont listen to you' , color=discord.Color.red()))

@tree.command(name = "botrequest", description = "Request a new feature in discord bot", guild=discord.Object(id=guildId)) #Return total messages
async def botrequest(interaction, request:str):  
     
    if "'" in request or '"' in request or "#" in request:
        await interaction.response.send_message(embed=discord.Embed(title='Don\'t use any special characters.' , color=discord.Color.red()))
        print(f'{interaction.user} tried using ">{request}<"')
    else:
        await interaction.response.send_message(embed=discord.Embed(title='Your request has been recorded and will be reviewed.' ,description=f'Request: {request}', color=discord.Color.yellow()))
        print(interaction.id, request, interaction.user, interaction.created_at )
        if cnx.is_connected(): 
            insert_query = "Insert into lnd_requests (id, request, user, request_datetime, type) values (%s, %s, %s, %s, %s)" 
            insert_values = (interaction.id, str(request), str(interaction.user), datetime.now().isoformat(), 'bot request')

            cursor.execute(insert_query, insert_values)
            cnx.commit()
            print("Inserted row:", insert_values) 

@tree.command(name = "plexrequest", description = "Request a new movie or show in plex", guild=discord.Object(id=guildId)) #Return total messages
async def plexrequest(interaction, request:str):  
     
    if "'" in request or '"' in request or "#" in request:
        await interaction.response.send_message(embed=discord.Embed(title='Don\'t use any special characters.' , color=discord.Color.red()))
        print(f'{interaction.user} tried using ">{request}<"')
    else:
        await interaction.response.send_message(embed=discord.Embed(title=f'Please allow up to 48 hours to have this added, unless told otherwise.',description=f'Request: {request}' , color=discord.Color.yellow()))
        print(interaction.id, request, interaction.user, interaction.created_at )
        if cnx.is_connected(): 
            insert_query = "Insert into lnd_requests (id, request, user, request_datetime, type) values (%s, %s, %s, %s, %s)" 
            insert_values = (interaction.id, str(request), str(interaction.user), datetime.now().isoformat(), 'plex request')

            cursor.execute(insert_query, insert_values)
            cnx.commit()
            print("Inserted row:", insert_values) 

@tree.command(name = "plexsearch", description = "Search the Plex library for a movie", guild=discord.Object(id=guildId))  
async def plexsearch(interaction, request:str): 
    movie_list = []     
    counter = 1
    movies = plex.library.section('Movies')
    for video in movies.search(f'{request}'):
        movie_list.append(f'{counter}. {video.title}')
        print(f'{video.title}') 
        counter +=1
    movie_lists = '\n'.join(movie_list)
    if movie_list:
        await interaction.response.send_message(embed=discord.Embed(title=f'Movies based on "{request}"', description=columnar([[movie_lists]], no_borders=True), color=discord.Color.green()))
    else:
        await interaction.response.send_message(embed=discord.Embed(title=f'There are no movies matching "{request}"' , color=discord.Color.red()))    

@tree.command(name = "plexsuggest", description = "Suggest a random movie", guild=discord.Object(id=guildId))  
async def plexsuggest(interaction, genre_input:str): 
    try: 
        movie_genres = ['Action', 'Adventure', 'Animation', 'Anime', 'Biography', 'Children', 'Comedy', 'Crime', 'Documentary', 'Drama', 'Family', 'Fantasy', 'History', 'Horror', 'Indie', 'Martial Arts', 'Music', 'Musical', 'Mystery', 'Romance', 'Science Fiction', 'Short', 'Sport', 'Suspense', 'Talk Show', 'Thriller', 'TV Movie', 'War', 'Western']
        count = movie_genres.count(genre_input.capitalize())
        if count == 0:
            await interaction.response.send_message(embed=discord.Embed(title=f'Please enter a valid Genre.' , description = ', '.join(movie_genres),color=discord.Color.red()))
        elif count != 0:

            movie_list = []      
            movies = plex.library.section('Movies')
            for video in movies.search(genre=[genre_input]):
                movie_list.append(video.title) 

            rrvideo = random.choice(movie_list)

            rvideo = movies.search(title=rrvideo)[0] 

            millis = rvideo.duration 
            minutes=(millis/(1000*60))%60
            minutes = int(minutes)
            hours=int((millis/(1000*60*60))%24)
            duration = f'{hours}hr {minutes}min' 

            #writers_list = [writer.tag for writer in rvideo.writers] 
            #writers = ', '.join(writers_list)  

            directors_list = [director.tag for director in rvideo.directors] 
            directors = ', '.join(directors_list) 
             
            #producers_list = [producer.tag for producer in rvideo.producers] 
            #producers = ', '.join(producers_list)  

            genres_list = [genre.tag for genre in rvideo.genres] 
            genres = ', '.join(genres_list)   

            embed = discord.Embed(title=f'Random movie to watch in the {genre_input.capitalize()} genre', colour=discord.Colour.dark_orange())
            embed.add_field(name="Title", value=rvideo.title, inline=False) 
            embed.add_field(name="Content Rating", value=rvideo.contentRating, inline=True) 
            embed.add_field(name="Year", value=rvideo.year, inline=True) 
            embed.add_field(name="Duration", value=duration, inline=True) 
            embed.add_field(name="Genres", value=genres, inline=True)   
            embed.add_field(name="Studio", value=rvideo.studio, inline=True) 
            embed.add_field(name="Director", value=directors, inline=True)   
            #embed.add_field(name="Writer", value=writers, inline=True) 
            #embed.add_field(name="Producer", value=producers, inline=True)   
            embed.add_field(name="Audience Rating", value=rvideo.audienceRating, inline=True) 
            embed.add_field(name="Rotten Rating", value=rvideo.rating, inline=True)   
    
            await interaction.response.send_message(embed=embed)  
        else:
            pass 
    except Exception as e:
        return str(e)

@tree.command(name = "chatgpt", description = "Ask AI a question", guild=discord.Object(id=guildId))  
async def chatgpt(interaction, question:str): 
    try: 
        openai.api_key = vars['APIKey']
        messages = [ {"role": "system", "content": 
              "You are a intelligent assistant."} ]
        message = question
        if message:
                messages.append(
                    {"role": "user", "content": message},
                )
                chat = openai.ChatCompletion.create(
                    model="gpt-3.5-turbo", messages=messages
                )
        reply = chat.choices[0].message.content
        
        messages.append({"role": "assistant", "content": reply})
        await interaction.response.defer(ephemeral=False)
        await asyncio.sleep(20)
        await interaction.followup.send(embed=discord.Embed(title=question , description = reply, color=discord.Color.red()))
    except Exception as e:
        print(e)
        print(question)

############################################################################ Functions ############################################################################
#Store message function
def store_message(author, content, reaction, edited_at, created_at, server, id, channel, author_id): 
    if cnx.is_connected():
         
        new_reaction = ' '.join(str(e) for e in reaction)
        insert_query = "Insert into lnd_messages (author, content, reactions, edited_dte, created_dte, server, id, channel, author_id, reaction_count) values (%s, %s, %s, %s,%s, %s, %s, %s, %s, %s)" 
        insert_values = (str(author), str(content), new_reaction, edited_at, created_at, str(server), id, str(channel), author_id, 0)

        cursor.execute(insert_query, insert_values)
        cnx.commit()
        print("Inserted row:", insert_values) 

#Edit message function
def message_edit(after_content, before_id, before_content):  
    if cnx.is_connected(): 
        update_query = f'update lnd_messages set content = "{after_content}", edited_dte = "{datetime.now().isoformat()}" where id={before_id}' #, edited_dte = "{str(after.edited_at)}"  
        try:
            cursor.execute(update_query)
            cnx.commit()
        except Exception as e:
            print("Error at:")
            print(e)  

#Refresh user table function
def user_refresh():
    for guild in client.guilds:
        for m in guild.members:
            print(m, m.id, m.joined_at, m.created_at, m.bot)
            if cnx.is_connected():  
                insert_query = "Insert into lnd_users (user_id, user_name, joined_dtm, created_dtm, isbot) values (%s, %s, %s, %s, %s)" 
                insert_values = (m.id, str(m), str(m.joined_at), str(m.created_at), m.bot)
                try:
                    cursor.execute(insert_query, insert_values)
                    cnx.commit()
                    print("Inserted record: ", insert_values)
                except Exception as e:
                    print("Error at:")
                    print(e)  

#Store reaction function     
def store_reaction(message_id, emoji, user_id, member, channel_id): 
    if cnx.is_connected():  
        insert_query = "Insert into lnd_reaction (message_id, emoji, user_id, member_name, channel_id, active, last_modified_dtm) values (%s, %s, %s, %s, %s, %s, %s)" 
        insert_values = (message_id, str(emoji), user_id, str(member), channel_id, 1, datetime.now().isoformat())  
        try:
            cursor.execute(insert_query, insert_values)
            cnx.commit()
            print("Inserted row: ", insert_values)  
        except Exception as e:
            print("Error at:")
            print(e) 

#Removing reaction function 
def remove_reaction(message_id, emoji, user_id):  
    if cnx.is_connected():    
        #Update messages with reaction count
        update_message_query = f'update lnd_messages set reaction_count = reaction_count - 1 where id = {message_id}'
        update_reaction_query = f'update lnd_reaction set active = 0 where message_id = {message_id} and user_id = {user_id} and emoji = "{emoji}"' 
        try:
            cursor.execute(update_message_query)
            cnx.commit()
            print("Updated message record to: ", update_message_query)
            cursor.execute(update_reaction_query)
            print("Updated reaction count to: ", update_reaction_query)
            cnx.commit() 
        except Exception as e:
            print("Error at:")
            print(e) 

#Refresh channel data function
def chnl_refresh():
    for channel in client.guilds:
        for c in channel.text_channels:  
            if cnx.is_connected():  
                insert_query = "Insert into lnd_channels (id, name, created_at, server, category) values (%s, %s, %s, %s, %s)" 
                insert_values = (c.id, str(c.name), str(c.created_at), str(c.guild), str(c.category))
                try:
                    cursor.execute(insert_query, insert_values)
                    cnx.commit()
                    print("Inserted record: ", insert_values)
                except Exception as e:
                    print("Error at:")
                    print(e)  

#Refresh channels
def chnl_refresh():
    for channel in client.guilds:
        for c in channel.text_channels:  
            if cnx.is_connected():  
                insert_query = "Insert into lnd_channels (id, name, created_at, server, category) values (%s, %s, %s, %s, %s)" 
                insert_values = (c.id, str(c.name), str(c.created_at), str(c.guild), str(c.category))
                try:
                    cursor.execute(insert_query, insert_values)
                    cnx.commit()
                    print("Inserted record: ", insert_values)
                except Exception as e:
                    print("Error at:")
                    print(e)  

#Record deleted messages function
def message_delete(id):    
    update_query = f'update lnd_messages set isactive = 0  where id = {id}'  
    try:
        cursor.execute(update_query)
        cnx.commit() 
    except Exception as e:
        print("Error at:")
        print(e) 

############################################################################ Client Events ############################################################################
@client.event
async def on_message(message): #When message is sent, store in message table
    try:
        if message.author == client.user:
            return 
        else:
            store_message(message.author, message.content, message.reactions, message.edited_at, message.created_at, message.guild, message.id, message.channel,message.author.id)
    except Exception as e:
        return str(e)
    
    #try:
    #    if message.content == "/user" and message.author.id == 553337834090659899: 
    #        user_refresh()
    #    if "peter\'s mom" in message.content.lower() or 'peters mom' in message.content.lower() or 'Peters mom' in (message.content) or "Peter\'s mom" in (message.content):
    #        await message.add_reaction('<:penis:285904916742930432>') #<:penis:285904916742930432>
    #except Exception as e:
    #    return str(e)
    try:
        if message.content == "/user" and message.author.id == 553337834090659899: 
            user_refresh()
        if 'peter’s mom' in message.content.lower() or 'peters mom' in message.content.lower() or 'peter\'s mom' in message.content.lower():
            await message.add_reaction(vars["MOM"]) #<:penis:285904916742930432> 
            response_list = ['Milk, milk, lemonade, \'round the corner fudge is made', 'She gave me a rimjob', 'Now both of them can call me \'daddy\'', 'I be destroying her ass', 'I bang her harder than a screen door in a hurricane', 'She know who really gives it to her good', 'Best door mat I\'ve ever found on the side of the road', 'I\'ve been considering a better model lately', 'Should be illegal for ass to be that easy']
            resp = random.choice(response_list)
            await message.reply(resp, mention_author=True) 
    except Exception as e:
        return str(e)
    try:
        if message.author.id == 553337834090659899 and message.content == "/chan":
            chnl_refresh()
    except Exception as e:
        return str(e)
    
@client.event
async def on_message_edit(before,after): #When user edits message
    try:
        message_edit(after.content, before.id, before.content)
        print(f"Updated contents from: '{before.content}' to '{after.content}'") 
    except Exception as e:
        return str(e)

@client.event
async def on_raw_message_delete(payload): #When user deletes message
    try:
        print(f'{payload.message_id} was deleted by {payload.cached_message.author.name} ') 
        message_delete(payload.message_id) 
    except Exception as e:
        return str(e)

@client.event 
async def on_raw_reaction_add(payload): #When user adds reaction to message
    try:
        if client.user.id != payload.user_id:
            store_reaction(payload.message_id, payload.emoji, payload.user_id, payload.member, payload.channel_id)  
    except Exception as e:
        return str(e)

@client.event
async def on_raw_reaction_remove(payload): #When User removes reaction
    try:
        if client.user.id != payload.user_id:
            remove_reaction(payload.message_id, payload.emoji, payload.user_id)  
    except Exception as e:
        return str(e)
    
@client.event
async def on_ready():
    print(f"Bot | Status:   Operational")
    print(f"Bot | ID:       {format(client.user.id)}")
    print(f"Bot | Name:     {format(client.user.name)}")
    print(f"Bot | Guilds:   {len(client.guilds)}") 
    print(f"Bot is ready to use") 
    await client.change_presence(activity=discord.Activity(type=discord.ActivityType.listening, name="King Jengar Only ")) 
    await tree.sync(guild=discord.Object(id=guildId))
    print(discord.utils.get(client.guilds)) 
    for guild in client.guilds:
        for member in guild.members:
            print(member)

client.run(token)